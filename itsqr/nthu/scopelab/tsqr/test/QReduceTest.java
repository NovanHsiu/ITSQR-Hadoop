/**
 Modify from nthu.scopelab.tsqr.QRFirstJob, keep only reducer part
**/
package nthu.scopelab.tsqr.test;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.Date;
import java.lang.Math;
import java.io.InputStreamReader;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.fs.FileStatus;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FSDataOutputStream;

import nthu.scopelab.tsqr.math.QRFactorMultiply;
import nthu.scopelab.tsqr.matrix.MatrixWritable;
import nthu.scopelab.tsqr.matrix.LMatrixWritable;
import nthu.scopelab.tsqr.TSQRunner;
import nthu.scopelab.tsqr.QRFirstJob;


public class QReduceTest extends TSQRunner{
  
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
	
	JobConf job = new JobConf(getConf(), QReduceTest.class);
	job.setJobName("QReduceTest-Iter-"+thenumber);
	job.setInputFormat(SequenceFileInputFormat.class);
	job.setOutputFormat(SequenceFileOutputFormat.class);
	

	job.setMapperClass(IdentityMapper.class);		
	job.setReducerClass(QRFirstJob.MergeReducer.class); 
    job.setOutputKeyClass(IntWritable.class);
	
	long InputSize = 0;
	FileStatus[] fstat = FileSystem.get(job).listStatus(new Path(rinput),new QRFactorMultiply.MyPathFilter("part"));
	Path Paths[] = new Path[fstat.length];
	for(int j=0;j<fstat.length;j++)
	{
	 InputSize+=fstat[j].getLen();
	 Paths[j] = fstat[j].getPath();
	}
	long SplitSize = mis*1024*1024; //mis: max input split size
	job.setNumMapTasks((int)(InputSize/SplitSize)+1);
    job.setOutputValueClass(LMatrixWritable.class);
    job.setNumReduceTasks(Integer.parseInt(stages[i]));
	job.setInt(COLUMN_SIZE,Integer.parseInt(colsize));
	
    FileSystem.get(job).delete(new Path(routput+thenumber), true);
	if(i!=0)
     FileInputFormat.setInputPaths(job, Paths);
	else
	 FileInputFormat.setInputPaths(job, new Path(rinput));
	FileOutputFormat.setOutputPath(job, new Path(routput+thenumber));
	//multiple output of firstQ
	MultipleOutputs.addNamedOutput(job,
                                     QF_MAT,
                                     SequenceFileOutputFormat.class,
                                     IntWritable.class,
                                     LMatrixWritable.class);
	
    JobClient.runJob(job);
	rinput = routput+thenumber;
	}
     return 0;
    }
	
	public static void main(String[] args) throws Exception
	{
	 //argument: -input -output -reduceSchedule -colsize -mis
	 long start, end;
	 start = new Date().getTime();
	 ToolRunner.run(new Configuration(), new QReduceTest(), args);
	 end = new Date().getTime();
	 System.out.println("QReduceTest Execution Time: "+(end-start));
	}
}