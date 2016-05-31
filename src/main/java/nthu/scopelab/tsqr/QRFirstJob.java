/**
 * Merge sub matrix and do tall and skinny QR (TSQR) factorization.
--------------
 Input: 
	Sub matrices of A
		<key, value> : <Id of submatrix, submatrix>
 Output: 
	1. R matrix
		<key, value> : <Id of map task, R matrix>
	2. first Q matrix
		<key, value> : <input key, first Q submatrix>
**/
package nthu.scopelab.tsqr;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.Date;
import java.lang.Math;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FSDataOutputStream;

import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.LMatrixWritable;
import nthu.scopelab.tsqr.math.QRF;
import nthu.scopelab.tsqr.math.QRFactorMultiply;
import nthu.scopelab.tsqr.TSQRunner.fileGather;
import nthu.scopelab.tsqr.TSQRunner.Checker;

import org.netlib.lapack.Dgeqrf;
import org.netlib.lapack.Dorgqr;
import org.netlib.util.intW;
import nthu.scopelab.tsqr.math.QRF;

public class QRFirstJob extends TSQRunner{

  public static enum QF_COUNTER {
  compute_time
 };

  public static final String V_FACTOR_OUTPUT = "v.factor.output";
  public static final String COLUMN_SIZE = "column.zise";
  public static final String QF_MAT = "QFM";
  private static final boolean debug = false;
  
  public int run(String[] args) throws Exception {
                       
    String inputfile = getArgument("-input",args);                
    String outputfile = getArgument("-output",args);        
    String reduceSchedule = getArgument("-reduceSchedule",args);
    String colsize = getArgument("-colsize",args);
	int mis = Integer.valueOf(getArgument("-mis",args));
	
    String stages[] = reduceSchedule.split(",");
	String rinput = inputfile;
    String routput = outputfile+"/iter-r-";
	
	for(int i=0;i<stages.length;i++)
	{
	String thenumber = Integer.toString(i+1);
	
	JobConf job = new JobConf(getConf(), QRFirstJob.class);
	job.setJobName("QRFirstJob-Iter-"+thenumber);
	job.setInputFormat(SequenceFileInputFormat.class);
	job.setOutputFormat(SequenceFileOutputFormat.class);
	
	if(i==0)
		job.setMapperClass(MergeMapper.class);
	else
		job.setMapperClass(IdentityMapper.class);
		
	job.setReducerClass(MergeReducer.class); 
    job.setOutputKeyClass(IntWritable.class);
	FileSystem fs = FileSystem.get(job);

	Path Paths[];
	fileGather fgather = new fileGather(new Path(rinput),"part",fs);
	Paths = fgather.getPaths();
	mis = Checker.checkMis(mis,fgather.getInputSize(),fs);
	job.setNumMapTasks(fgather.recNumMapTasks(mis));
	
    job.setOutputValueClass(LMatrixWritable.class);
    job.setNumReduceTasks(Integer.parseInt(stages[i]));
	job.setInt(COLUMN_SIZE,Integer.parseInt(colsize));
	
    fs.get(job).delete(new Path(routput+thenumber), true);	
    FileInputFormat.setInputPaths(job, Paths);
	FileOutputFormat.setOutputPath(job, new Path(routput+thenumber));
	
	//FileOutputFormat.setCompressOutput(job, true);
    //FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
	//SequenceFileOutputFormat.setOutputCompressionType(job,CompressionType.BLOCK);
	//multiple output of firstQ
	MultipleOutputs.addNamedOutput(job,
                                     QF_MAT,
                                     SequenceFileOutputFormat.class,
                                     IntWritable.class,
                                     LMatrixWritable.class);
	
    RunningJob rj = JobClient.runJob(job);
	rinput = routput+thenumber;
	//Counters counters = rj.getCounters();
	//Counters c1 = counters.findCounter(QF_COUNTER.compute_time);
	//Counters c2 = counters.findCounter(QF_COUNTER.compute_time);
	//System.out.println(c1.getValue());
	}
     return 0;
    }
public static class MergeJob extends MapReduceBase
{
		protected OutputCollector<IntWritable,LMatrixWritable> output;
		protected int colsize;		
		protected int TaskId;
		protected List<cmDenseMatrix> matrix_buffer = new ArrayList<cmDenseMatrix>();
		protected List<Integer> key_buffer = new ArrayList<Integer>();
		protected List<Integer> matRowSize_buffer = new ArrayList<Integer>();
		protected long tt1, tt2, t1, t2 ,mosoutputTime=0, moscloseTime=0, computeTime=0;
		protected int numRows = 0, numCols = 0;
		protected IntWritable okey = new IntWritable();
		protected LMatrixWritable ovalue = new LMatrixWritable();
		protected MultipleOutputs qmos;
		protected String Mapred;
		@Override
		public void configure(JobConf job){
		 tt1 = new Date().getTime();
		 Mapred = job.get("mapred.task.id").split("_")[3];
         TaskId = Integer.parseInt(job.get("mapred.task.id").split("_")[4]);
		 colsize = job.getInt(COLUMN_SIZE,-1);
		 qmos = new MultipleOutputs(job);
        }
        
   public void collect(IntWritable key, LMatrixWritable value) throws IOException
   {
			//store sub Matrix
			numCols=value.matNumColumns();
			numRows+=value.matNumRows();
		    matrix_buffer.add(value.getDense().copy());	
			key_buffer.add(new Integer(key.get()));
			matRowSize_buffer.add(new Integer(value.matNumRows()));
   }
   
     @Override
        public void close() throws IOException {
		if(!matrix_buffer.isEmpty())
		{
			//initial a DenseMatrix As numRows x numCols
			cmDenseMatrix As = new cmDenseMatrix(numRows,numCols);
			System.out.println("mR size: "+numRows+", "+numCols);
			//put the matrix_buffer data into DenxeMatrix As
			int curRowbegingIndex = 0;
			
			while(!matrix_buffer.isEmpty())
			{
			 cmDenseMatrix dmat = matrix_buffer.remove(0);
			 for(int i=0;i<dmat.numRows();i++)
			  for(int j=0;j<dmat.numColumns();j++)
			  {
			   As.set(i+curRowbegingIndex,j,dmat.get(i,j));
			  }
			 curRowbegingIndex+=dmat.numRows();
			}
			
			// do QR factorization for As
			QRF qrf = QRF.factorize(As);
			cmDenseMatrix outputQ = qrf.getQ();
			cmDenseMatrix outputR = qrf.getR();
			//output the Q and R
			
			//output Q
			 int curRowIndex = 0;
			 cmDenseMatrix outputSplitQ = new cmDenseMatrix(new double[matRowSize_buffer.get(0)*numCols*2],matRowSize_buffer.get(0),numCols);
			 int srow_size;
			 //split matrix
			 while(!key_buffer.isEmpty())
			 {
			  srow_size =  matRowSize_buffer.remove(0);
			  
			  outputSplitQ.set(outputSplitQ.getData(),srow_size,numCols);
			  for(int i=0;i<srow_size;i++)
			   for(int j=0;j<numCols;j++)
			    outputSplitQ.set(i,j,outputQ.get(i+curRowIndex,j));
				
			  okey.set(key_buffer.remove(0));
			  ovalue.set(outputSplitQ);
			  t1 = new Date().getTime(); //debug
			  qmos.getCollector(QF_MAT,null).collect(okey,ovalue);
			  t2 = new Date().getTime(); //debug
			  //System.out.println("Qs: "+(t2-t1));
			  mosoutputTime+=(t2-t1);
			  curRowIndex+=srow_size;
			 }
			 
			 t1 = new Date().getTime(); //debug
			 qmos.close();
			 t2 = new Date().getTime(); //debug			 
			 moscloseTime+=(t2-t1);
			 
			okey.set(TaskId);
			ovalue.set(outputR);
			
			output.collect(okey,ovalue);
			
			tt2 = new Date().getTime();
			if(debug)
			{
			 System.out.println("mos Output Time: "+mosoutputTime);
			 System.out.println("QRF Total Time: "+(tt2-tt1));
			}
			
		}//matrix_buffer.isEmpty()
    }//close
			
}
public static class MergeMapper 
        extends MergeJob implements Mapper<IntWritable, LMatrixWritable, IntWritable, LMatrixWritable>{			
						
        public void map(IntWritable key, LMatrixWritable value, OutputCollector<IntWritable, LMatrixWritable> output, Reporter reporter)
            throws IOException {
			      this.output = output;
			      collect(key,value);
        }
    }
    
public static class MergeReducer 
        extends MergeJob implements Reducer<IntWritable, LMatrixWritable, IntWritable, LMatrixWritable>{				
        public void reduce(IntWritable key, Iterator<LMatrixWritable> values, OutputCollector<IntWritable, LMatrixWritable> output, Reporter reporter)
            throws IOException {
			      this.output = output;
            while (values.hasNext()) 
            {
			     collect(key,values.next());
            }
        }
    }
}