/**
 * Do Q^t*B multiplication (This B matrix is not B matrix of ssvd)
--------------
 Input:
	1. Sub matrices of B
		<key, value> : <Id of submatrix, submatrix>
	2. first Q matrix
		<key, value> : <Id of submatrix, first Q submatrix>
 Output: 
	1. Q^t*B matrix
		<key, value> : <Id of submatrix, Q^t*B submatrix>
	2. final Q matrix (alternative output)
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
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.io.SequenceFile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.ToolRunner;

import no.uib.cipr.matrix.Matrix;
import nthu.scopelab.tsqr.matrix.LMatrixWritable;
import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.math.QRFactorMultiply;
import nthu.scopelab.tsqr.BuildQJob.QIndexPair;
import nthu.scopelab.tsqr.TSQRunner.fileGather;
import nthu.scopelab.tsqr.TSQRunner.Checker;

@SuppressWarnings("unchecked")
public class QmultiplyJob extends TSQRunner{
  public static final String SCHEDULE_NUM = "schedule.number";
  public static final String OUTPUT_Q = "output.q";
  public static final String A_COL_SIZE = "a.col.size";
  public static final String Q_MAT = "Q";
  public static final String QRF_DIR = BuildQJob.QRF_DIR;
  private static boolean debug = true;
  public int run(String[] args) throws Exception {
    boolean isDense = true, outputQ = false;
	
    String inputpath = getArgument("-input",args);                
    String outputfile = getArgument("-output",args);        
    String reduceSchedule = getArgument("-reduceSchedule",args);
	String qrfpath = getArgument("-first_q",args);
	String outputQStr = getArgument("-output_q",args);
	String Acolsize = getArgument("-acolsize",args);
	int mis = Integer.valueOf(getArgument("-mis",args));	
	
    String stages[] = reduceSchedule.split(",");
    String output = outputfile;
		 
	if(outputQStr.equals("true"))
	 outputQ = true;
	else
	 outputQ = false;
	 	
	JobConf job = new JobConf(getConf(), QmultiplyJob.class);
	job.setInputFormat(SequenceFileInputFormat.class);
	job.setOutputFormat(SequenceFileOutputFormat.class);
	
	FileSystem fs = FileSystem.get(job);
	Path Paths[];
	fileGather fgather = new fileGather(new Path(inputpath),"",fs);
	Paths = fgather.getPaths();
	mis = Checker.checkMis(mis,fgather.getInputSize(),fs);
	job.setNumMapTasks(fgather.recNumMapTasks(mis));
	
	job.set(QRF_DIR,qrfpath);
	job.setInt(SCHEDULE_NUM,stages.length);
	job.setInt(A_COL_SIZE,Integer.valueOf(Acolsize));
	job.setBoolean(OUTPUT_Q,outputQ);
	fs.get(job).delete(new Path(output), true);		
    FileInputFormat.addInputPath(job, new Path(inputpath));
	FileOutputFormat.setOutputPath(job, new Path(output));
	job.setJobName("QmultiplyJob");	
	job.setMapperClass(MultiplyMapper.class);		
	job.setCombinerClass(MergeReducer.class);
	job.setReducerClass(MergeReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(LMatrixWritable.class);	
    job.setNumReduceTasks(1);	 
	
	if(outputQ)
	{
	 MultipleOutputs.addNamedOutput(job,
                                     Q_MAT,
                                     SequenceFileOutputFormat.class,
                                     IntWritable.class,
                                     LMatrixWritable.class);
	}		
    JobClient.runJob(job);
		
     return 0;
    }
	
	public static class readAtoBuildQMethod extends BuildQJob.BuildQMethod
    {
		private Map<Integer,QIndexPair> QFMap = new HashMap<Integer,QIndexPair>();
		
		@Override
		public void configure(JobConf job){
		 try{
		 super.configure(job);
		 fs = FileSystem.get(job);
		 
		 SequenceFile.Reader reader = null;
		 IntWritable lkey;
		 //build QF Map: 13/12/24
		 String TaskId = job.get("mapred.task.id").split("_")[4];
		 TaskId = TaskId.substring(1,TaskId.length());
		 
		 FileStatus[] QFStatus = fs.listStatus(new Path(qrfpath+"/iter-r-1"), new QRFactorMultiply.MyPathFilter(QRFirstJob.QF_MAT+"-m-"+TaskId));
		 for(int i=0;i<QFStatus.length;i++)
		 {
		    reader = new SequenceFile.Reader(fs, QFStatus[i].getPath(), fs.getConf());			
		    lkey = (IntWritable) reader.getKeyClass().newInstance();
			long offset = reader.getPosition();
		    while(reader.next(lkey))
			{
		     QFMap.put(lkey.get(),new QIndexPair(QFStatus[i].getPath(),offset));
			 offset = reader.getPosition();
			} 
		 }
		 	 		 
		}
		catch(Exception e)
		{
		 e.printStackTrace();
		 throw new NullPointerException("Exception!");
		}		
		}
		

		public void BuildQ(IntWritable key) throws IOException{
			//find the first QF matrix
			SequenceFile.Reader reader = null;
			IntWritable lkey;
			LMatrixWritable lvalue;
			try{
			QIndexPair qip = QFMap.get(key.get());
			reader = new SequenceFile.Reader(fs, qip.path, fs.getConf());			
		    lkey = (IntWritable) reader.getKeyClass().newInstance();
		    lvalue = (LMatrixWritable) reader.getValueClass().newInstance();	
			reader.seek(qip.offset);
		    reader.next(lkey,lvalue);
			Q_TaskId = Integer.valueOf(qip.path.getName().split("-")[2]); //the mark of first Q for second (subsequent) Q to identify and to complete the matrix multiplication of sub Q
			reader.close();
			super.BuildQ(lkey,lvalue);//,Q_TaskId);						
			}
			catch(Exception e)
			{
			 e.printStackTrace();
			 throw new NullPointerException("cp buildQ: debug!");
			}
		}
	}
	
public static class MultiplyMapper //only work on iteration 1 (index 0)
        extends readAtoBuildQMethod implements Mapper<IntWritable, LMatrixWritable, IntWritable, LMatrixWritable>{		
		protected int Acolsize;
		private long time1, time2, time3, totalTime1 = 0, totalTime2 = 0;
		private boolean outputQ;
		private cmDenseMatrix QtB = null;
		private MultipleOutputs mos;
		private long initMemory = 0;//debug
		private LMatrixWritable ovalue = new LMatrixWritable();
		@Override
		public void configure(JobConf job){
		 initMemory = Runtime.getRuntime().freeMemory();
		 try{
		 super.configure(job);		
		 if(Runtime.getRuntime().freeMemory()<initMemory/2) //debug
		  throw new NullPointerException("cp1: OOM!");
		 Acolsize = job.getInt(A_COL_SIZE,1);
		 outputQ = job.getBoolean(OUTPUT_Q,false); 
		 mos = new MultipleOutputs(job);		 		
		}
		catch(Exception e)
		{
		 e.printStackTrace();
		 throw new NullPointerException("Exception!");
		}
		
		}
				
        public void map(IntWritable key, LMatrixWritable value, OutputCollector<IntWritable, LMatrixWritable> output, Reporter reporter)
            throws IOException {
			//Build Q
			BuildQ(key);
			if(outputQ)
			{
			  ovalue.setLMat(value.getLongArray(),Q);
			  mos.getCollector(Q_MAT, null).collect(key, ovalue);
			}
			//do Qt x B multiplication
			int qcn = Q.numColumns();
			int acn = value.getDense().numColumns();			
			if(QtB==null)
			 QtB = new cmDenseMatrix(new double[qcn*acn*2],qcn,acn);
			else if(QtB.getData().length<qcn*acn)
			 QtB = new cmDenseMatrix(new double[qcn*acn*2],qcn,acn);
			else 
			 QtB.set(QtB.getData(),qcn,acn);
			 
			if(Runtime.getRuntime().freeMemory()<initMemory/2)//debug
		     throw new NullPointerException("cp2: OOM!");
			QtB = QRFactorMultiply.Multiply("T","N",Q,value.getDense(),QtB);
			if(Runtime.getRuntime().freeMemory()<initMemory/2)//debug
		     throw new NullPointerException("cp3: OOM!");
			
			ovalue.setLMat(value.getLongArray(),QtB);
			output.collect(key,ovalue);
        }
		
		@Override
		public void close() throws IOException {
			mos.close();
		}
		
    }

public static class MergeReducer 
        extends MapReduceBase implements Reducer<IntWritable, LMatrixWritable, IntWritable, LMatrixWritable>{
		protected cmDenseMatrix mat = null;
		protected OutputCollector<IntWritable,LMatrixWritable> output;
		
        public void reduce(IntWritable key, Iterator<LMatrixWritable> values, OutputCollector<IntWritable, LMatrixWritable> output, Reporter reporter)
            throws IOException {			
			if(mat==null)
			{
			 this.output = output;
			 mat = values.next().getDense();
			}
            while (values.hasNext()) 
            {
				cmDenseMatrix lmat =  values.next().getDense();
			    mat = mat.add(lmat);
            }
        }
		
		@Override
		public void close() throws IOException
		{
		 Random rand = new Random();
		 if(mat!=null)
		  output.collect(new IntWritable(rand.nextInt()),new LMatrixWritable(mat));
		}
    }
	
}
