/**
 * Split the A matrix into sub matrices depend on matrix number list that produced by subMatrixCountJob.
--------------
 Input: 
	1. A matrix in text file. 
		<key,value> : <offset of this line in the text file, one line in the text file>
	2. Number list in text file (use one reducer in this job that only output one file). This file produced by subMatrixCountJob.
		<key,value> : <Id of map tasks, matrix number list(Include the order and row size of each sub matrices)>
 Output: 
	Sub matrices of A
		<key, value> : <Id of submatrix, submatrix>
**/
package nthu.scopelab.tsqr;

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

import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.LMatrixWritable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class subMatrixGenerateJob extends TSQRunner{

  private static final Logger log = LoggerFactory.getLogger(subMatrixGenerateJob.class);
  
  public static final String SUBROW_SIZE = "sub.row.size";
  public static final String PART_LIST = "part.list";
  public static final String COL_SIZE = "col.size";
  public int run(String[] args) throws Exception {
    	
    String inputfile = getArgument("-input",args);                
    String outputfile = getArgument("-output",args);
	//Partition List record the partition method for dividing the input matrix into mutiple sub matrices 
	//which is generated by subMatrixCountJob
	String partList = getArgument("-part_list",args);
    String reducerNum = getArgument("-reducer_number",args);
    String subrow_size = getArgument("-subrow_size",args);
    String col_size = getArgument("-colsize",args);
	int mis = Integer.valueOf(getArgument("-mis",args));
	
	JobConf job = new JobConf(getConf(), subMatrixGenerateJob.class);
	job.setJobName("subMatrixGenerate");
	job.setInputFormat(TextInputFormat.class);
	job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setMapperClass(MergeMapper.class);
	//job.setCombinerClass(MergeCombiner.class);
	job.setReducerClass(MergeReducer.class);
	job.setMapOutputKeyClass(IntPairWritable.class);
    job.setMapOutputValueClass(LMatrixWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(LMatrixWritable.class);
	job.setOutputKeyComparatorClass(IntPairComparator.class);
	
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
	job.set(PART_LIST,partList);
	job.set(COL_SIZE,col_size);
    FileSystem.get(job).delete(new Path(outputfile), true);
    FileInputFormat.setInputPaths(job, new Path(inputfile));
    FileOutputFormat.setOutputPath(job, new Path(outputfile));
	
	//FileOutputFormat.setCompressOutput(job, true);
    //FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	//job.setMapOutputCompressorClass(GzipCodec.class);
    JobClient.runJob(job);    
     return 0;
    }
	
	public static class MergeMapper 
        extends MapReduceBase implements Mapper<LongWritable, Text, IntPairWritable, LMatrixWritable>{
		
		protected int subrow_size;		
		protected int count = 0;
		
		protected int blocknum = 0, col_size;
		protected List<String> curRowBuffer = new ArrayList<String>();
		protected List<String> indexList = new ArrayList<String>();
		protected List<String> rownumList = new ArrayList<String>();
		protected String mapTaskId = "";
		
		private IntPairWritable okey = new IntPairWritable();
		private LMatrixWritable ovalue = new LMatrixWritable();
		private cmDenseMatrix submatrix = null;
		@Override
		public void configure(JobConf job){
         mapTaskId = job.get("mapred.task.id").split("_")[4];
		 subrow_size = Integer.parseInt(job.get(SUBROW_SIZE));
		 col_size = Integer.parseInt(job.get(COL_SIZE));
		 String partlist = job.get(PART_LIST);
		 //read the partition list
		 Path listPath =new Path(partlist);			 
		 try{
		 FileSystem fs = FileSystem.get(job);
		 BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(listPath)));
		 try {
			String line = "";
			String[] secPart;
			log.info("Test subMatrixGenerate");
			while (line != null){
				line=br.readLine();
				if(line==null)
				 break;
				if(Integer.parseInt(line.split("	")[0]) != Integer.parseInt(mapTaskId))
				 continue;
				 
				secPart = line.split("	")[1].split(" "); //split tab, split space
				for(int i=0;i<secPart.length;i+=2)
				{
				 indexList.add(secPart[i]);
				 rownumList.add(secPart[i+1]);
				}
				break;
			}
		}//try2
		 finally {	
			br.close();
		 }
		 }//try1
		 catch(Exception e)
		 {
		  e.printStackTrace();
		 }
        }
						
        public void map(LongWritable key, Text value, OutputCollector<IntPairWritable, LMatrixWritable> output, Reporter reporter)
            throws IOException {
			curRowBuffer.add(value.toString());
			if(curRowBuffer.size()== Integer.parseInt(rownumList.get(count)))
			{
			 submatrix = new cmDenseMatrix(curRowBuffer.size(),col_size);
			 if(submatrix==null)
			  submatrix = new cmDenseMatrix(new double[curRowBuffer.size()*col_size*2],curRowBuffer.size(),col_size);
			 else
			 {
			  if(curRowBuffer.size()>submatrix.numRows())
			   submatrix = new cmDenseMatrix(new double[curRowBuffer.size()*col_size*2],curRowBuffer.size(),col_size);
			  else
			   submatrix = new cmDenseMatrix(submatrix.getData(),curRowBuffer.size(),col_size);
			 }
			 
			 SetTheMatrix(curRowBuffer,submatrix);
			 okey.setV(Integer.parseInt(indexList.get(count)),Integer.parseInt(mapTaskId));
			 ovalue.set(submatrix);
			 output.collect(okey,ovalue);
			 count++;
			}
        }
				
		public static void SetTheMatrix(List<String> RowBuffer,cmDenseMatrix submatrix)
		{		 
				String[] row;
				for(int i=0;i<submatrix.numRows();i++)
				{
					row = RowBuffer.remove(0).split(" ");
					for(int j=0;j<submatrix.numColumns();j++)
					{
					submatrix.set(i,j,Double.parseDouble(row[j]));
					}
				}
		}
		
    }
	
	/*public static class MergeCombiner 
        extends MapReduceBase implements Reducer<IntPairWritable, LMatrixWritable, IntPairWritable, LMatrixWritable>{
		public void reduce(IntPairWritable key, Iterator<LMatrixWritable> values, OutputCollector<IntPairWritable, LMatrixWritable> output, Reporter reporter)
            throws IOException {
			cmDenseMatrix mMatrix = null;
			while (values.hasNext()) { 
             cmDenseMatrix newMatrix = values.next().get();
			 if(mMatrix==null && values.hasNext()==false)
			  mMatrix = newMatrix;
			 else
			  mMatrix = MergeReducer.mergeMatrix(mMatrix,newMatrix);
		    }
			if(mMatrix!=null)
			  output.collect(key,new LMatrixWritable(mMatrix));
			else
			 throw new NullPointerException("submatrix LMatrixWritable: " + Integer.toString(key.getV1()));
		}
	}*/
	 
	public static class MergeReducer 
        extends MapReduceBase implements Reducer<IntPairWritable, LMatrixWritable, IntWritable, LMatrixWritable>{
		
		private IntWritable okey = new IntWritable();
		private LMatrixWritable ovalue = new LMatrixWritable();
        public void reduce(IntPairWritable key, Iterator<LMatrixWritable> values, OutputCollector<IntWritable, LMatrixWritable> output, Reporter reporter)
            throws IOException {
			cmDenseMatrix mMatrix = null;
			while (values.hasNext()) {
             cmDenseMatrix newMatrix = values.next().getDense();
			 if(mMatrix==null && values.hasNext()==false)
			  mMatrix = newMatrix;
			 else
			  mMatrix = mergeMatrix(mMatrix,newMatrix);
		    }
			if(mMatrix!=null)
			{
			  okey.set(key.getV1()); 
			  ovalue.set(mMatrix);
			  output.collect(okey,ovalue);
			}
			else
			 throw new NullPointerException("submatrix LMatrixWritable: " + Integer.toString(key.getV1()));
        }
		
		
		public static cmDenseMatrix mergeMatrix(cmDenseMatrix mMatrix,cmDenseMatrix newMatrix)
		{
		 if(mMatrix==null)
			mMatrix = newMatrix.copy();
		 else
		 {
		  int rowsize = mMatrix.numRows()+newMatrix.numRows();
		  int colsize = mMatrix.numColumns();
		  cmDenseMatrix newmergerMatrix = new cmDenseMatrix(rowsize,colsize);
		  //set old matrix
		  for(int i=0;i<mMatrix.numRows();i++)
		   for(int j=0;j<colsize;j++)
		    newmergerMatrix.set(i,j,mMatrix.get(i,j));
		  //set new matrix
		  for(int i=0;i<newMatrix.numRows();i++)
		   for(int j=0;j<colsize;j++)
		    newmergerMatrix.set(i+mMatrix.numRows(),j,newMatrix.get(i,j));
		  mMatrix = newmergerMatrix;
		  boolean debug = true;
		 }
		 return mMatrix;
		}
		
    }
 
  //customer WritableComparable class
	public static class IntPairWritable implements WritableComparable {
       // Some data
       private int v1;
       private int v2;
       IntPairWritable(){
	    v1 = 0;
		v2 = 0;
	   }
	   
	   IntPairWritable(int v1,int v2){
	    this.v1 = v1;
		this.v2 = v2;
	   }
	   @Override
       public void write(DataOutput out) throws IOException {
         out.writeInt(v1);
         out.writeInt(v2);
       }
	   @Override
       public void readFields(DataInput in) throws IOException {
         v1 = in.readInt();
         v2 = in.readInt();
       }
	   @Override
       public int compareTo(Object o) {
	     IntPairWritable ipwo = (IntPairWritable)o;
         int thisValue = this.v1;
         int thatValue = ipwo.v1;
         return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
       }
	   @Override
	   public int hashCode() {
         return v1;
       }
	   public void setV(int v1,int v2)
	   {
	    this.v1 = v1;
		this.v2 = v2;
	   }
       public int getV1()
	   {
	    return v1;
	   }
	   public int getV2()
	   {
	    return v2;
	   }
     }
	 
	public static class IntPairComparator extends WritableComparator {
		protected IntPairComparator() {
			super(IntPairWritable.class);
		}
     
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int v1a = readInt(b1, s1);
			int v1b = readInt(b2, s2);
			return (v1a < v1b ? -1 : (v1a==v1b ? 0 : 1));
		}
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new subMatrixGenerateJob(), args);
	}
}
