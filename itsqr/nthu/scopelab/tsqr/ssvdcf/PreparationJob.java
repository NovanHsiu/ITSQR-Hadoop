/**
 * Transform user rating data into multiple sub-matrices
--------------
 Input: 
	1. user rating 
		<key,value> : <offset of this line in the text file,[userId] [itemId] [rating]>
 Output: 
	Sub matrices of A matrix
		<key, value> : <Id of submatrix, submatrix>
**/
package nthu.scopelab.tsqr.ssvdcf;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.lang.Math;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileStatus;

import nthu.scopelab.tsqr.matrix.LMatrixWritable;
import nthu.scopelab.tsqr.TSQRunner;

import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.sparse.SparseVector;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreparationJob extends TSQRunner{

  private static final Logger log = LoggerFactory.getLogger(PreparationJob.class);
  
  public static final String SUBROW_SIZE = "sub.row.size";
  public static final String PART_LIST = "part.list";
  public static final String COL_SIZE = "col.size";
  public int run(String[] args) throws Exception {
    	
    String inputfile = getArgument("-input",args);                
    String outputfile = getArgument("-output",args);

    String reducerNum = getArgument("-reduceTasks",args);
    String subrow_size = getArgument("-subRowSize",args);
	int mis = Integer.valueOf(getArgument("-mis",args));
	
	JobConf job = new JobConf(getConf(), PreparationJob.class);
	job.setJobName("PreparationJob");
	job.setInputFormat(TextInputFormat.class);
	job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setMapperClass(SplittedMapper.class);
	job.setCombinerClass(VectorCombiner.class);
	job.setReducerClass(MatrixReducer.class);
	job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(LMatrixWritable.class);
	
	long InputSize = 0;
	FileStatus[] fstat = FileSystem.get(job).listStatus(new Path(inputfile));
	for(int j=0;j<fstat.length;j++)
	{
	 InputSize+=fstat[j].getLen();
	}
	long SplitSize = mis*1024*1024; //mis: max input split size
	job.setNumMapTasks((int)(InputSize/SplitSize)+1);
    job.setNumReduceTasks(Integer.parseInt(reducerNum));
	
	job.set(SUBROW_SIZE,subrow_size);
    FileSystem.get(job).delete(new Path(outputfile), true);
    FileInputFormat.setInputPaths(job, new Path(inputfile));
    FileOutputFormat.setOutputPath(job, new Path(outputfile));
	
    JobClient.runJob(job);    
     return 0;
    }
	
	public static class SplittedMapper 
        extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{
		
		private IntWritable okey = new IntWritable();
		private Text ovalue = new Text();
				
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter)
            throws IOException {
			String str = value.toString();
			String[] strArr = str.split("[+-,!?;:'\"\\p{Space}]+");
			int userId = Integer.valueOf(strArr[0]);
			String item = strArr[1];
			String rating = strArr[2];
			
			okey.set(userId);
			ovalue.set(item+":"+rating);
			output.collect(okey,ovalue);
        }
		
    }
	
	public static class VectorCombiner 
        extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{
		
		private Text ovalue = new Text();
				
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter)
            throws IOException {
			String outputStr = "";
			while(values.hasNext())
			{
			 outputStr = outputStr+values.next().toString()+",";
			}
			ovalue.set(outputStr);
			output.collect(key,ovalue);
        }
    }
	
	public static class MatrixReducer 
        extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, LMatrixWritable>{
		
		private int subrowsize;
		private List<Integer> curUserIdBuffer = new ArrayList<Integer>();
		private List<Vector> curRowBuffer = new ArrayList<Vector>();
		private long[] longArr = null;
		
		private IntWritable okey = new IntWritable();
		private LMatrixWritable ovalue = new LMatrixWritable();
		private OutputCollector<IntWritable, LMatrixWritable> output;
		
		@Override
		public void configure(JobConf job){
			subrowsize = Integer.parseInt(job.get(SUBROW_SIZE));
		}
		
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, LMatrixWritable> output, Reporter reporter)
            throws IOException {
			this.output = output;
			
			String str = "";
			while(values.hasNext())
			{
			 str = str+values.next().toString()+",";
			}
			String strArr[] = str.split(",");
			
			SparseVector userVector = new SparseVector(Integer.MAX_VALUE);
			for(int i=0;i<strArr.length;i++)
			{
				String itemRating[] = strArr[i].split(":");
				if(itemRating.length<2)
				 continue;
				int index = Integer.valueOf(itemRating[0]);
				float element = Float.valueOf(itemRating[1]);
				userVector.set(index,element);
			}
			curUserIdBuffer.add(key.get());
			curRowBuffer.add(userVector);
			
			if(curRowBuffer.size()>=subrowsize*2)
			{
			int rowbuffersize = curRowBuffer.size()/2;
			FlexCompRowMatrix submatrix = new FlexCompRowMatrix(rowbuffersize,Integer.MAX_VALUE);
			if(longArr==null)
			longArr = new long[rowbuffersize*2];
			else if(longArr.length<rowbuffersize)
			longArr = new long[rowbuffersize*2];
			//set the output value
			SetTheOutputValue(curUserIdBuffer,longArr,curRowBuffer,submatrix,rowbuffersize);
			//write out
			ovalue.setLMat(longArr,rowbuffersize,submatrix);
			okey.set((int)longArr[0]);
			output.collect(okey,ovalue);
			}
        }
		
		@Override
		public void close() throws IOException
		{
		
		if(curRowBuffer.size()>0)
		{
		 int rowbuffersize = curRowBuffer.size();
		 FlexCompRowMatrix submatrix = new FlexCompRowMatrix(rowbuffersize,Integer.MAX_VALUE);
		 if(longArr==null)
		  longArr = new long[rowbuffersize*2];
		 else if(longArr.length<rowbuffersize)
		 longArr = new long[rowbuffersize*2];
		 //set the output value
		 SetTheOutputValue(curUserIdBuffer,longArr,curRowBuffer,submatrix,rowbuffersize);
		 //write context
		 ovalue.setLMat(longArr,rowbuffersize,submatrix); 
		 okey.set((int)longArr[0]);
		 output.collect(okey,ovalue);
		}
		
		}
    }
	
	protected static void SetTheOutputValue(List<Integer> UserIdBuffer, long[] longArr, List<Vector> RowBuffer, FlexCompRowMatrix submatrix, int rowbuffersize)
	{
		for(int i=0;i<rowbuffersize;i++)
		{
		 longArr[i] = UserIdBuffer.remove(0);
		 submatrix.setRow(i,(SparseVector) RowBuffer.remove(0));
		}
	}
 	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PreparationJob(), args);
	}
}
