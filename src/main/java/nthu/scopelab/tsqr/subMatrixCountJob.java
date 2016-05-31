/**
 * Give each sub matrix an identification number that depend on sub matrix order in matrix A. 
 * Output file is sub matrix number list.
--------------
Input: 
	A matrix in text file. 
		<key,value> : <offset of this line in the text file, one line in the text file>
Output: 
	Number list in text file (use one reducer in this job that only output one file). 
		<key,value> : <Id of map tasks, matrix number list(Include the order and row size of each sub matrices)>
"""
**/
package nthu.scopelab.tsqr;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileStatus;

public class subMatrixCountJob extends TSQRunner{
  
  public static final String SUBROW_SIZE = "sub.row.size";
  public static final String COL_SIZE = "col.size";
  public int run(String[] args) throws Exception {
                       
    String inputfile = getArgument("-input",args);                
    String outputfile = getArgument("-output",args);   
	
    String subrow_size = getArgument("-subrow_size",args);
    String col_size = getArgument("-colsize",args);
	int mis = Integer.valueOf(getArgument("-mis",args));
	//String MapperNumTasks = getArgument("-mapper_num_tasks",args);
	
	
	JobConf job = new JobConf(getConf(), subMatrixCountJob.class);
	job.setJobName("subMatrixCount");
	job.setInputFormat(TextInputFormat.class);
	job.setOutputFormat(TextOutputFormat.class);
    job.setMapperClass(CountMapper.class);
	//job.setCombinerClass(CountCombiner.class);
    job.setReducerClass(CountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
	
	long InputSize = 0;
	FileStatus[] fstat = FileSystem.get(job).listStatus(new Path(inputfile));
	for(int j=0;j<fstat.length;j++)
	{
	 InputSize+=fstat[j].getLen();
	}
	long SplitSize = mis*1024*1024; //mis: max input split size
	job.setNumMapTasks((int)(InputSize/SplitSize)+1);
    job.setNumReduceTasks(1);
	job.set(SUBROW_SIZE,subrow_size);
	job.set(COL_SIZE,col_size);
    FileSystem.get(job).delete(new Path(outputfile), true);
    FileInputFormat.setInputPaths(job, new Path(inputfile));
    FileOutputFormat.setOutputPath(job, new Path(outputfile));
	
    //job.setMapOutputCompressorClass(GzipCodec.class);
    JobClient.runJob(job); 
     return 0;
    }
	
	public static class CountMapper 
        extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
		
		protected OutputCollector<Text,Text> output;
		
        protected String mapTaskId;
		protected String inputFile;
		protected int subrow_size, col_size;
		protected int RowNum = 0;
		
		@Override
		public void configure(JobConf job) {
         mapTaskId = job.get("mapred.task.id").split("_")[4];
		 Path inputPath = new Path(new String(job.get("map.input.file")));
		 inputFile = inputPath.getName();
		 subrow_size = Integer.parseInt(job.get(SUBROW_SIZE));
		 col_size = Integer.parseInt(job.get(COL_SIZE));
        }
		
        public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter)
            throws IOException {
			this.output = output;
			if(value.toString().split(" ").length!=col_size)
			 throw new IllegalArgumentException("matrix column size not match the \"colsize\" variable: " + Integer.toString(value.toString().split(" ").length));
			RowNum++;
        }
		
		@Override
			
        public void close() throws IOException {
            if (output != null) {
				inputFile = inputFile+" ";
				for(int i=0;i<(4-mapTaskId.length());i++)				 
				 inputFile = inputFile+"0";
               output.collect(new Text(inputFile+mapTaskId),new Text(Integer.toString(RowNum)));			   
            }
        }
		
    }
	
	//Add Combiner for this MapReduce Job could not obviously improve performance (probably reduce performance)
	/*public static class CountCombiner 
        extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter)
            throws IOException {
			int RowAccumulate = 0;
			while (values.hasNext()) {
			  Text val = values.next();
			  RowAccumulate+=Integer.parseInt(val.toString());
			}
		  output.collect(key,new Text(Integer.toString(RowAccumulate)));
		}
	}*/
	public static class CountReducer 
        extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
		
		protected OutputCollector<Text,Text> output;
		
        protected int totalrow = 0, RowBuffer = 0, preRowBuffer = 0;
		protected int subrow_size;
		protected List<Integer> mapperIdList = new ArrayList<Integer>(); 
		protected List<Integer> mbeginList = new ArrayList<Integer>(); //begining number of sub matrix of each map tasks
		protected List<Integer> rownumList = new ArrayList<Integer>(); //amount of sub matrix of each map tasks 
		
		@Override
		public void configure(JobConf job) {
		 subrow_size = Integer.parseInt(job.get(SUBROW_SIZE));
        }
		
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text,Text> output, Reporter reporter)
            throws IOException {
			this.output = output;
            while (values.hasNext()) {
			 Text val = values.next();
			 int mapid = Integer.parseInt(key.toString().split(" ")[1]);
			 int rownum = Integer.parseInt(val.toString());
			 
			 preRowBuffer = RowBuffer;
			 RowBuffer+= rownum;
			 totalrow+= rownum;
			 
			 while(RowBuffer>= subrow_size)
			 {
			  mapperIdList.add(mapid);
			  rownumList.add(subrow_size-preRowBuffer);
			  rownum-=(subrow_size-preRowBuffer);			  
			  RowBuffer-= subrow_size;
			  preRowBuffer = 0;
			 }			 
			 if(RowBuffer>0)
			 {
			  mapperIdList.add(mapid);
			  rownumList.add(rownum);
			 }
			}
        }
		
		@Override
        public void close() throws IOException {
         if (output != null) {		 
		 int rowacc=0, rowbuffer=0;
		 int mapid, premapid, rownum;
		 int mbegin;
		 int blockindex = 1;
		 String valuetext = "";
		 premapid = mapperIdList.get(0).intValue();
		 Text okey = new Text(), ovalue = new Text();
		 for(int i=0;i<rownumList.size();i++)
		 {
		  mapid = mapperIdList.get(i).intValue();
		  rownum = rownumList.get(i).intValue();
		  rowacc+=rownum;
		  rowbuffer+=rownum;	
		  if(mapid!=premapid)
		  {
		   okey.set(Integer.toString(premapid));
		   ovalue.set(valuetext);
		   output.collect(okey,ovalue);
		   valuetext = "";		   
		  }
		  valuetext = valuetext + Integer.toString(blockindex) + " " + Integer.toString(rownum) + " ";
		  premapid = mapid;
		  if(rowbuffer>=subrow_size)
		  {
		   rowbuffer-=subrow_size;
		   if(totalrow-rowacc>=subrow_size)
		    blockindex++;
		  }		  
		 }//for
		 if(valuetext.length()>0)
		 {
		  okey.set(Integer.toString(premapid));
		  ovalue.set(valuetext);
		  output.collect(okey,ovalue);
		 }
         }//if output != null
        }//close()
		
	}
	   
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new subMatrixCountJob(), args);
  }
}
