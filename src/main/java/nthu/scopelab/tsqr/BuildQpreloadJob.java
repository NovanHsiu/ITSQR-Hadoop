/**
 * Do Q = Q1 x Q2 x ... x Qk matrix multiplication
 * The final Q multiplied by multiple first Q
 * The different part of BuildQJob and BuildQpreloadJob that is the former loads all Qs matrix in memory and the latter loads Qs matrix from HDFS when there need to use.
--------------
 Input: 
	first Q matrix
		<key, value> : <Id of submatrix, first Q submatrix>
 Output: 
	final Q matrix
		<key, value> : <Id of submatrix, final Q submatrix>
**/
package nthu.scopelab.tsqr;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.lang.Math;
import java.security.InvalidAlgorithmParameterException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.SequenceFile;

import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.MatrixEntry;
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix;
import no.uib.cipr.matrix.sparse.FlexCompColMatrix;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.DenseVector;

import nthu.scopelab.tsqr.matrix.LMatrixWritable;
import nthu.scopelab.tsqr.matrix.SparseRowBlockAccumulator;
import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.TSQRunner;
import nthu.scopelab.tsqr.math.QRFactorMultiply;
import nthu.scopelab.tsqr.TSQRunner.fileGather;
import nthu.scopelab.tsqr.TSQRunner.Checker;

@SuppressWarnings("unchecked")
public class BuildQpreloadJob extends TSQRunner{
  private static final boolean debug = false;
  public static final String QRF_DIR = "qrf.dir";
  public static final String SCHEDULE_NUM = "schedule.number";

  private BuildQpreloadJob() {
  }
  
  public static void run(Configuration conf,String inputPathStr,String outputPathStr,
				String reduceSchedule,int mis) throws Exception {
    boolean outputQ = true;
	
    	
    String stages[] = reduceSchedule.split(",");
	String qrfpath = inputPathStr + "/iter-r-1";
	Path outputPath = new Path(outputPathStr);
	
	JobConf job = new JobConf(conf, BuildQpreloadJob.class);
	job.setInputFormat(SequenceFileInputFormat.class);
	job.setOutputFormat(SequenceFileOutputFormat.class);
	job.setInt(SCHEDULE_NUM,stages.length);
	job.set(QRF_DIR,inputPathStr);
	FileSystem.get(job).delete(outputPath, true);		
    
	FileOutputFormat.setOutputPath(job, outputPath);
	job.setJobName("BuildQpreloadJob");
	
	job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(LMatrixWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(LMatrixWritable.class);	
	job.setMapperClass(BuildQMapper.class);
	
	FileSystem fs = FileSystem.get(job);
	Path Paths[];
	fileGather fgather = new fileGather(new Path(qrfpath),QRFirstJob.QF_MAT+"-m-",fs);
	Paths = fgather.getPaths();
	mis = Checker.checkMis(mis,fgather.getInputSize(),fs);
	job.setNumMapTasks(fgather.recNumMapTasks(mis));
	
    job.setNumReduceTasks(0);
	
	FileInputFormat.setInputPaths(job,Paths);
	
	 RunningJob rj = JobClient.runJob(job);
    }
	
	public static class BuildQMethod extends MapReduceBase
    {
	 protected String qrfpath, filename; 
	 protected int Q_TaskId = -1; //Task Id of first QR Factorization Map Task
	 protected FileSystem fs;
	 protected int oiter;
	 protected int scheduleNumber;
	 protected int taskId;	
	 protected cmDenseMatrix Q = null;
	 private List<Map<Integer,QsMatPair>> QIndexMapList = new ArrayList<Map<Integer,QsMatPair>>();	 
	 protected long t1, t2, computationTime = 0;
	 
	@Override
	public void configure(JobConf job)
	{
		filename = job.get("map.input.file");
		filename = filename.substring(filename.lastIndexOf("/")+1);
		if(filename.split("-").length>2)
		 Q_TaskId = Integer.valueOf(filename.split("-")[2]); //the mark of first Q for second (subsequent) Q to identify and to complete the matrix multiplication of sub Q
		
		 taskId = Integer.parseInt(job.get("mapred.task.id").split("_")[4]);
		 try{
		 t1 = new Date().getTime();
		 qrfpath = job.get(QRF_DIR);
		 scheduleNumber = job.getInt(SCHEDULE_NUM,1);
		 fs = FileSystem.get(job);	
		  //build Index Map: 13/12/24
		 SequenceFile.Reader reader = null;
		 oiter = 1;
		 while(fs.exists(new Path(qrfpath+"/iter-r-"+Integer.toString(oiter)))) //level of iterative MapReduce Job
		 {
			Map<Integer,QsMatPair> QIndexMap = new HashMap<Integer,QsMatPair>();
		    FileStatus[] lastQStatus = fs.listStatus(new Path(qrfpath+"/iter-r-"+Integer.toString(oiter)), new QRFactorMultiply.MyPathFilter(QRFirstJob.QF_MAT+"-r-"));
			for(int i=0;i<lastQStatus.length;i++) //output Q of reduce task of iterative MapReduce Job
			{
		    reader = new SequenceFile.Reader(fs, lastQStatus[i].getPath(), fs.getConf());			
		    IntWritable lkey = (IntWritable) reader.getKeyClass().newInstance();
			LMatrixWritable lvalue = (LMatrixWritable) reader.getValueClass().newInstance();
		    while(reader.next(lkey,lvalue))
			{
			 QIndexMap.put(lkey.get(),new QsMatPair(Integer.valueOf(lastQStatus[i].getPath().getName().split("-")[2]),lvalue.getDense().copy()));
			}		
			}
			QIndexMapList.add(QIndexMap);
			oiter++;
		 }
		 t2 = new Date().getTime();
		 System.out.println("Time of Build QIndexMapList: "+(t2-t1));
		}
		catch(Exception e)
		{
		 e.printStackTrace();
		 throw new NullPointerException("Exception!");
		}		 
	}
	
	public void BuildQ(IntWritable key, LMatrixWritable value) throws IOException
	{
		//"value" the input parameter is first Q
		//read Qs and multiply to Q
		if(Q==null)
		{
		 Q = new cmDenseMatrix(new double[value.getDense().numRows()*value.getDense().numColumns()*2],value.getDense().numRows(),value.getDense().numColumns());
		}
		else
		{
		 Q.set(Q.getData(),value.getDense().numRows(),value.getDense().numColumns());
		}
			
		int preTaskId = Q_TaskId; // this id link two Q between different iterative level to complete Final Q matrix by matrix multiplication
		cmDenseMatrix firstQ = value.getDense();
		cmDenseMatrix tempMat;
		try{
			 for(int i=0;i<QIndexMapList.size();i++)
			 {
			 QsMatPair qip = QIndexMapList.get(i).get(preTaskId);		
			 cmDenseMatrix secondQ = qip.Qs;
			 
			 //do matrix multiplication to get Q: Q = Q1 x Q2 x Q3 ...
			 //t1 = new Date().getTime();
			 QRFactorMultiply.Multiply("N","N",firstQ,secondQ,Q);
			 tempMat = firstQ;
			 firstQ = Q;
			 Q = tempMat;
			 //t2 = new Date().getTime();
			 //computationTime+=(t2-t1);
			 preTaskId = qip.taskId;
			 }
			 Q = firstQ;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			throw new NullPointerException("Error!");
		}
		
	}
	
   }
	
	public static class BuildQMapper //only work on iteration 1 (index 0)
        extends BuildQMethod implements Mapper<IntWritable, LMatrixWritable, IntWritable, LMatrixWritable>{
		private long start, end;
		private long confstart, mapstart, time1, time2, tag0, tag1, mapAcc = 0;
		private LMatrixWritable ovalue = new LMatrixWritable();
		private Reporter reporter;
		
		@Override
		public void configure(JobConf job)
		{
		 confstart = new Date().getTime();
		 tag0 = confstart;
		 super.configure(job);
		 time1 = new Date().getTime();

		 tag1 = time1;
		}
		
        public void map(IntWritable key, LMatrixWritable value, OutputCollector<IntWritable, LMatrixWritable> output, Reporter reporter)
            throws IOException {
			this.reporter = reporter;
			t1 = new Date().getTime();
			BuildQ(key,value);
			t2 = new Date().getTime();
			computationTime+=(t2-t1);		
			ovalue.setLMat(value.getLongArray(),Q);
			output.collect(key,ovalue);
        }
		
		@Override
		public void close() throws IOException {
		time2 = new Date().getTime();
		System.out.println("computationTime: "+computationTime);
		System.out.println("totalTime: "+(time2-confstart));
		}
		
    }
	
	public static class QsMatPair{
	
	public int taskId;
	public cmDenseMatrix Qs;
	
	public QsMatPair(int taskId,cmDenseMatrix Qs)
	{
	 this.taskId = taskId;
	 this.Qs = Qs;
	}	
	}
}