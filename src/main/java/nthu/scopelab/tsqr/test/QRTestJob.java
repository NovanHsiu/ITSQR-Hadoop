/* This code used to verify our QR factorization is correct.
  First, Do matrix operations A - Q x R = A'. Finally will output a double value which is accumulate of A' matrix all elements.
  If Q and R matrix are correct, the output value is 0 zero. (or much less than 0)
*/
package nthu.scopelab.tsqr.test;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.io.SequenceFile;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.ToolRunner;

import no.uib.cipr.matrix.Matrix;
import nthu.scopelab.tsqr.matrix.MatrixWritable;
import nthu.scopelab.tsqr.matrix.LMatrixWritable;
import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.math.QRFactorMultiply;
import nthu.scopelab.tsqr.BuildQJob.QIndexPair;
import nthu.scopelab.tsqr.BuildQJob;
import nthu.scopelab.tsqr.TSQRunner;
import nthu.scopelab.tsqr.QmultiplyJob;

@SuppressWarnings("unchecked")
public class QRTestJob extends TSQRunner{
  public static final String SCHEDULE_NUM = "schedule.number";
  public static final String R_PATH = "r.path";
  public static final String A_COL_SIZE = "a.col.size";
  public static final String Q_MAT = "Q";
  public static final String QRF_DIR = BuildQJob.QRF_DIR;
  private static boolean debug = true;
  public int run(String[] args) throws Exception {
	
    String inputpath = getArgument("-input",args);                
    String outputfile = getArgument("-output",args);        
    String reduceSchedule = getArgument("-reduceSchedule",args);
	String qrfpath = getArgument("-first_q",args);
	String Acolsize = getArgument("-acolsize",args);
	String rpath = getArgument("-rpath",args);
	int mis = Integer.valueOf(getArgument("-mis",args));	
	
    String stages[] = reduceSchedule.split(",");
    String output = outputfile;
		 
	JobConf job = new JobConf(getConf(), QRTestJob.class);
	job.setInputFormat(SequenceFileInputFormat.class);
	job.setOutputFormat(TextOutputFormat.class);
	
	long InputSize = 0;
	FileStatus[] fstat = FileSystem.get(job).listStatus(new Path(inputpath));
	for(int i=0;i<fstat.length;i++)
	{
	 InputSize+=fstat[i].getLen();
	}
	long SplitSize = mis*1024*1024; //mis: max input split size
	job.setNumMapTasks((int)(InputSize/SplitSize)+1);
	
	job.set(QRF_DIR,qrfpath);
	job.set(R_PATH,rpath);
	job.setInt(SCHEDULE_NUM,stages.length);
	job.setInt(A_COL_SIZE,Integer.valueOf(Acolsize));
	FileSystem.get(job).delete(new Path(output), true);		
    FileInputFormat.addInputPath(job, new Path(inputpath));
	FileOutputFormat.setOutputPath(job, new Path(output));
	job.setJobName("QRTestJob");	
	job.setMapperClass(MultiplyMapper.class);
	job.setReducerClass(printReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(MatrixWritable.class);	
    job.setNumReduceTasks(1);
	
    JobClient.runJob(job);
		
     return 0;
    }
	
public static class MultiplyMapper //only work on iteration 1 (index 0)
        extends QmultiplyJob.readAtoBuildQMethod implements Mapper<IntWritable, MatrixWritable, IntWritable, MatrixWritable>{		
		protected int Acolsize;
		private long time1, time2, time3, totalTime1 = 0, totalTime2 = 0;
		private double acc, ave;
		private cmDenseMatrix QR = null, Rmat = null, A, outputd = new cmDenseMatrix(1,1);
		private MatrixWritable ovalue = new MatrixWritable();
		
		@Override
		public void configure(JobConf job){
		 try{
		 super.configure(job);
		 String rpathstr = job.get(R_PATH);
		 Path rpath = new Path(rpathstr);
		 //load R Matrix
		 SequenceFile.Reader sreader = new SequenceFile.Reader(fs,rpath,fs.getConf());
		 try{
		  IntWritable skey = new IntWritable();
		  MatrixWritable svalue = new MatrixWritable();
		  sreader.next(skey,svalue);
		  Rmat = svalue.getDense();
		  sreader.close();
		 }
		 catch(Exception e)
		 {
		  e.printStackTrace();
		 }
		 Acolsize = job.getInt(A_COL_SIZE,1);		 		
		}
		catch(Exception e)
		{
		 e.printStackTrace();
		 throw new NullPointerException("Exception!");
		}
		
		}

        public void map(IntWritable key, MatrixWritable value, OutputCollector<IntWritable, MatrixWritable> output, Reporter reporter)
            throws IOException {
			
			//Build Q
			BuildQ(key,(LMatrixWritable)value);

			//do Qt x B multiplication
			int rows = value.getDense().numRows();
			int cols = value.getDense().numColumns();
					
			if(QR==null)
			 QR = new cmDenseMatrix(new double[rows*cols*2],rows,cols);
			else if(QR.getData().length<rows*cols)
			 QR = new cmDenseMatrix(new double[rows*cols*2],rows,cols);
			A = value.getDense();
			QR = QRFactorMultiply.Multiply("N","N",Q,Rmat.copy(),QR);

			acc = 0;
			for(int i=0;i<A.numRows();i++)
			 for(int j=0;j<A.numColumns();j++)
			 {
			  acc+=Math.abs(Math.abs(A.get(i,j))-Math.abs(QR.get(i,j)));
			 }
			ave = acc/(A.numRows()*A.numColumns());
			outputd.set(0,0,ave);
			ovalue.set(outputd);
			output.collect(key,ovalue);
        }
		
    }
	
	public static class printReducer 
        extends MapReduceBase implements Reducer<IntWritable, MatrixWritable, IntWritable, DoubleWritable>{
		private DoubleWritable ovalue = new DoubleWritable();
				
        public void reduce(IntWritable key, Iterator<MatrixWritable> values, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter)
            throws IOException {			
			
            while (values.hasNext()) 
            {
				cmDenseMatrix lmat =  values.next().getDense();
			    ovalue.set(lmat.get(0,0));
				output.collect(key,ovalue);
            }
        }
    }
	
   public static void main(String[] args) throws Exception {
    if(args.length<1)
	{
	 System.out.println("usage: -input <input_path> -output <output_path> -reduceSchedule <reduceschedule> -rpath <r_path> -first_q <first_q_path> -acolsize <col_size> -mis <max_input_split>");
	 return;
	}
    ToolRunner.run(new Configuration(), new QRTestJob(), args);
  }
}
