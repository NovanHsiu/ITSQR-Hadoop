/**
 * The Java implementation of the Tall-and-skinny QR factorization
 * Author: Hsiu-Cheng Yu
 * Reference: Tall and skinny QR factorizations in MapReduce architectures, https://github.com/dgleich/mrtsqr
 * Date: 2013
 */ 
package nthu.scopelab.tsqr;

import java.io.BufferedReader;
import java.util.Date;
import java.io.InputStreamReader;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;

import nthu.scopelab.tsqr.math.QRFactorMultiply;

/**
 * This driver code is used to run the TSQR algorithm.
 *
 * Example Usage:
 
   type 0: only output Q matrix:
	${HADOOP_HOME}/bin/hadoop jar ${TSQR_PACKAGE}/TSQR.jar nthu.scopelab.tsqr.TSQRunner \
	-input mat/100x5 -output tsqrOutput -subRowSize 4000 -reduceSchedule 4,1 \
	-type 0 -mis 64
   
   type 1: do matrix multiplication Qt x B:
	${HADOOP_HOME}/bin/hadoop jar ${TSQR_PACKAGE}/TSQR.jar nthu.scopelab.tsqr.TSQRunner \
	-input mat/100x5 -output tsqrOutput -subRowSize 4000 -reduceSchedule 4,1 \
	-type 1 -inputB mat/100x3 -mis 64
 
 * Input and Output variables:
	> input: path of A matrix in text file
	> inputB: path of B matrix in text file
	> output: 
	 -subMatrixCount_output: number list of sub A matrices, it record the order and row size of each sub matrices
	 -seqMatrix: split matrix A into sub matrices depend on number list
	 -QRf: matrix R in QR factorization of A and Q matrix in first step (we called that Q matrix fQ(first Q))
	 -Q: matrix Q in QR factorization (Q = fQ1 x fQ2 x ...)
     -QtB: hadoop Sequencefile of Q^T*B matrix
 */ 
public class TSQRunner extends Configured implements Tool {
  private static final long Milli = 1000;
  private long start, end, ts, te;
  @Override
  public int run(String[] args) throws Exception {
		ts = new Date().getTime();
        if (args.length == 0) {
            return printUsage();
        }
                   
        String inputpath = getArgument("-input",args);
        if (inputpath == null) {
            System.out.println("Required argument '-input' missing");
            return -1;
        }
                
        String outputpath = getArgument("-output",args);
        if (outputpath == null) {
            System.out.println("Required argument '-output' missing");
            return -1;
        }
		
		//reduceSchedule: Decided the execution times of MapReduce of FirstQJob and number of Reduce tasks of FirstQJob 
        String reduceSchedule = getArgument("-reduceSchedule",args);
        if (reduceSchedule == null) {
            reduceSchedule = "1";
        }
        //subRowSize: Row size of sub matrix As. These sub matrices split from A matrix.
        String subRowSize = getArgument("-subRowSize",args);
		
		String job1ReduceTasks = getArgument("-job1ReduceTasks",args);
		if (job1ReduceTasks == null) {
            job1ReduceTasks = "1";
        }
		
		String inputB = getArgument("-inputB",args);		
		String outputB = getArgument("-outputB",args);
		
		//outputQ: If the variable is true means that QMultiplyJob would output Q matrix
		String outputQ = getArgument("-outputQ",args);
		if (outputQ == null) {
            outputQ = "false";
        }
		
		//decides the number of Map tasks and size of file compute by each Map Tasks
		String MaxInputSplit = getArgument("-mis",args);
		if (MaxInputSplit == null) {
            MaxInputSplit = "64";
        }
		
		int type;
		String strType = getArgument("-type",args);
		if (strType == null) {
            type = 0;
        }
	else
	{
			type = Integer.valueOf(strType);
	}
		
		if(type==1)
		if (inputB == null) {
            System.out.println("Required argument '-inputB' missing");
            return -1;
        }
		
		String job1 = getArgument("-job1",args);
		String job2 = getArgument("-job2",args);
		String job3 = getArgument("-job3",args);
		if(job1==null)
		 job1 = "true";
		if(job2==null)
		 job2 = "true";
		if(job3==null)
		 job3 = "true";
        		
        String subMatrixCount_output = outputpath+"/seqMatrixCount";
		String seqMatrix_output = outputpath+"/seqMatrix";
		
		
		//Job1
		FileSystem fs = FileSystem.get(getConf());
		SequenceFileMatrixMaker mat_maker = new SequenceFileMatrixMaker();
		if(job1.equals("true"))
		{
		//Job 1-1: Create sub matrix number list
		//Job 1-2: Generate sequential sub matrix depend on number_list
		start = new Date().getTime();
		int info = ToolRunner.run(getConf(), mat_maker, new String[]{
              "-input", inputpath,
              "-subMatrixCount_output", subMatrixCount_output,
	      "-seqMatrix_output", seqMatrix_output,
              "-subRowSize", subRowSize,
	      "-mis",MaxInputSplit});	  
		end = new Date().getTime();
		System.out.println("PreparationJob: "+Long.toString((end-start)/Milli)+"s");
		if(info==-1)
		 return -1;
		//System.out.print(Long.toString((end-start)/Milli)+" ");
		}
		
		String colsize;
		if(job1.equals("true"))
		 colsize = mat_maker.getColSize();
		else
		{
		 FileStatus[] filestatus = fs.listStatus(new Path(inputpath));
         FSDataInputStream d = new FSDataInputStream(fs.open(filestatus[0].getPath()));
         BufferedReader reader = new BufferedReader(new InputStreamReader(d));
         String line = "";
	     line = reader.readLine();
		 colsize = Integer.toString(line.split(" ").length);
		 reader.close();
		}
				
		String QRf = outputpath+"/FirstQR";
		String finalQ = outputpath+"/Q";
		//Job 2: Merge sub matrix with TSQR and then output R, T and Householder vector
		if(job2.equals("true"))
		{
		start = new Date().getTime();
		ToolRunner.run(getConf(), new QRFirstJob(), new String[]{
              "-input", seqMatrix_output,
              "-output", QRf,
			  "-mis",MaxInputSplit,
			  "-colsize", colsize,
              "-reduceSchedule", reduceSchedule});		
		end = new Date().getTime();
		System.out.println("QRFirstJob: "+Long.toString((end-start)/Milli)+"s");
		//System.out.print(Long.toString((end-start)/Milli)+" ");
		}
		
		//Job3
		if(job3.equals("true"))
		{
		
		switch(type)
		{
		
		//type 0: only output Q		
		case 0:
			//Job3: Build Q Job
			start = new Date().getTime();
			BuildQJob.run(getConf(),QRf,finalQ,reduceSchedule,Integer.valueOf(MaxInputSplit)); //final parameter is input split size		
			end = new Date().getTime();
			System.out.println("BuildQJob: "+Long.toString((end-start)/Milli)+"s");
			//System.out.println(Long.toString((end-start)/Milli)+" ");	
			break; //type 0
		
		//type 1: do transpose(Q)*B
		case 1:		
			if(!inputB.equals(inputpath))
			{
			 subMatrixCount_output = outputpath+"/seqMatrixCountb";
			 seqMatrix_output = outputpath+"/seqMatrixb";			
			 if(!fs.exists(new Path(seqMatrix_output)))
			 {	
			  //Produce SequenceFile Matrix from Matrix B
			  int info = ToolRunner.run(getConf(), mat_maker, new String[]{
              "-input", inputpath,
              "-subMatrixCount_output", subMatrixCount_output,
			  "-seqMatrix_output", seqMatrix_output,
              "-subRowSize", subRowSize,
			  "-mis",MaxInputSplit});	  
			  if(info==-1)
			  return -1;			  
			 }
			}				
			start = new Date().getTime();
			//Job3:  do transpose(Q)*B
			String QtB = outputpath+"/QtB";
			ToolRunner.run(getConf(), new QmultiplyJob(), new String[]{
             "-input", seqMatrix_output,
			 "-first_q",QRf,
             "-output", QtB,
			 "-output_q",outputQ,
			 "-mis", MaxInputSplit,
             "-reduceSchedule", reduceSchedule,
			 "-acolsize",colsize});
			end = new Date().getTime();
			System.out.println("QmultiplyJob: "+Long.toString((end-start)/Milli)+"s");						
			break; //type1
		}//switch(type)
		}//if job3 == true
		te = new Date().getTime();
		System.out.println("TotalTime: "+Long.toString((te-ts)/Milli)+"s");
        return 0;
    }
	
	private static int printUsage() {
        System.out.println("usage: -input <filepath> -output <outputpath> [-subRowSize <int>] [-mis <int>] [-reduceSchedule <int>,<int>,...,1] [-inputB <filepath>] [-outputB <outputpath>]");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }
    
    public String getArgument(String arg, String[] args) {
        for (int i=0; i<args.length; ++i) {
            if (arg.equals(args[i])) {
                if (i+1<args.length) {
                    return args[i+1];
                } else {
                    return null;
                }
            }
        }
        return null;
    }
	
	public static class Checker{
	/**
	* Control the size of Map Max Input Split Size
	*
    * @param mis
    *  Map Max Input Split Size
    * @param datasize
    *  Size of all input data(file)
    * @param fs
    *  FileSystem
    * @throws IOException
    *  when IO condition occurs
    */
	public static int checkMis(int mis,long datasize,FileSystem fs) throws IOException
	{
	 /*int checkmis = -1;
	 long mb = 1024*1024;
	 long lmis = mis*mb;
	 OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
     //free memory size
     long freememory= ((com.sun.management.OperatingSystemMXBean)operatingSystemMXBean).getFreePhysicalMemorySize();
	 
	 DistributedFileSystem dfs = (DistributedFileSystem) fs;
	 DatanodeInfo[] live = dfs.getClient().datanodeReport(DatanodeReportType.LIVE);
	 
	 String mapredheap_str = fs.getConf().get("mapred.child.java.opts");
         String lastchar = String.valueOf(mapredheap_str.charAt(mapredheap_str.length()-1));
         long mapredheap = Long.valueOf(mapredheap_str.replaceAll("[^0-9]*",""));
         if(lastchar.matches("[mM]+"))
          mapredheap = mapredheap*1024*1024;
         else
          mapredheap = mapredheap*1024*1024*1024;
	 	 
	 //(physical machine free memory) > (mpared child heap size)*(datasize/mis+1)/(# of data nodes)
	 // Free Memory > Required Memory for per node (Assume free memory of master node and slave node are equal, because it only detect memory usage of master node.)
	 //System.out.println("FreeMemory: "+freememory+", RequiredMemory: "+mapredheap*(datasize/lmis+1)/live.length+" for per node of cluster");
	 if(freememory > mapredheap*(datasize/lmis+1)/live.length)
	  checkmis = mis;
	 else //mis is too small, enlarge and re-check
	  checkmis = checkMis(mis*2,datasize,fs);
	 if(checkmis!=mis)
		System.out.println("Argument \"mis\" is too small: "+mis+", automatically increace to: "+checkmis);
	 return checkmis;*/
	 //recent day, in the SSBDS this part has some weird error...
	 return mis;
	}
	
	/**
	* Control the row size of sub matrix
	*
    * @param srs
    *  sub row size (row size of sub matrix)
    * @param colsize
    *  column size of matrix
    * @param mis
    *  Map Max Input Split Size
    * @throws IOException
    *  when IO condition occurs
    */
	public static int checkSrs(int srs,int colsize,int mis)
	{
	 int checksrs = -1;
	 int mb = 1024*1024;
	 if((srs*colsize*8)/mb < mis)
	  checksrs = srs;
	 else
	  checksrs = checkSrs(srs/2,colsize,mis);
	 if(checksrs!=srs)
		System.out.println("Argument \"subRowSize\" is too large: "+srs+", automatically reduce to: "+checksrs);
	 return checksrs;
	}
	}
	
	//gather specific files from one directory
	public static class fileGather
	{
	 private long inputSize;
	 private Path[] paths;
	 
    /**
	* Gather specific files from one directory
	*
    * @param input
    *  path of input directory
    * @param namefilter
    *  partial beginging string of filenames, which would be gather
    * @param fs
    *  max input split size of map task
    * @throws IOException
    *  when IO condition occurs
    */	
	public fileGather(Path input,String namefilter,FileSystem fs) throws IOException
	{
	 FileStatus[] fstat = fs.listStatus(input,new QRFactorMultiply.MyPathFilter(namefilter));
	 gatherFile(fstat);
	}
	
	//Polymorphism Interface 2
	public fileGather(Path[] inputs,String namefilter,FileSystem fs) throws IOException
	{
	 FileStatus[] fstat = fs.listStatus(inputs,new QRFactorMultiply.MyPathFilter(namefilter));
	 gatherFile(fstat);
	}
	
	/**
	* Gather specific files from FileStatus
	*
	* @param fstat
	*  FileStatus
	*/
	private void gatherFile(FileStatus[] fstat)
	{
	  inputSize = 0;	
	  paths = new Path[fstat.length];	
	  for(int i=0;i<fstat.length;i++)
	  {
	   inputSize+=fstat[i].getLen();
	   paths[i] = fstat[i].getPath();
	  }
	 }
	
    /**
    * Retrun paths that has been gathered
    * @return paths
    *  file paths that are gathered
    */
	public Path[] getPaths()
	{
	 return paths;
	}
	
	/**
    * retrun size of input data
    * @return inputSize
    *  size of input data
    */
	public long getInputSize()
	{
	 return inputSize;
	}
	
	/**
    * Recommendate a number of map tasks from max input split size (mis)
    * @param mis
    *  max input split size of map task
    * @return integer number
    *  number of map tasks
    */
	public int recNumMapTasks(int mis)
	{
	 long splitSize = mis*1024*1024;
	 return (int)(inputSize/splitSize)+1;
	}
	}

  public static void main(String[] args) throws Exception {
    long start = new Date().getTime();
    ToolRunner.run(new Configuration(), new TSQRunner(), args);
    long end = new Date().getTime();
    //System.out.println("Total Time "+Long.toString((end-start)/Milli)+"s");
	System.out.println(Long.toString((end-start)/Milli));
  }
}
