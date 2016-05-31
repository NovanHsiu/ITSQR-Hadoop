/**
 * This Class used to turn matrix which is text file into Hadoop SequenceFile
 * Author: Hsiu-Cheng Yu
 * Date: 2013
 */ 
package nthu.scopelab.tsqr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import nthu.scopelab.tsqr.TSQRunner.fileGather;
import nthu.scopelab.tsqr.TSQRunner.Checker;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Date;

/**
 * This is used to turn matrix in text file into Hadoop SequenceFile
 *
 * Example Usage:
	{$HADOOP_HOME}/bin/hadoop jar {$TSQR_PACKAGE}/TSQR.jar nthu.scopelab.tsqr.SequenceFileMatrixMaker \
	-input mat/100kx100 -output tsqrOutput -subRowSize 4000
    
 * Input and Output variables:
	> input: path of A matrix in text file
	> inputB: path of B matrix in text file
	> output: 
	 -subMatrixCount_output: number list of sub A matrices, it record the order and row size of each sub matrices
	 -seqMatrix: split matrix A into sub matrices depend on number list
 */ 
public class SequenceFileMatrixMaker extends TSQRunner {
  
  private String subMatrixCount_output;		
  private String seqMatrix_output;
  private String MaxInputSplit;
  private String job1ReduceTasks;
  private String subRowSize;
  private String colsize;
  private String outputpath;
  private String inputpath;
  private FileSystem fs;
  
  public SequenceFileMatrixMaker()
  {
   
  }
  
  @Override
  public int run(String[] args) throws Exception {
        if (args.length == 0) {
            return printUsage();
        }
                   
        inputpath = getArgument("-input",args);
        if (inputpath == null) {
            System.out.println("Required argument '-input' missing");
            return -1;
        }
        subMatrixCount_output = getArgument("-subMatrixCount_output",args);
		if (subMatrixCount_output == null) {
            System.out.println("Required argument '-subMatrixCount_output' missing");
            return -1;
        }		
		seqMatrix_output = getArgument("-seqMatrix_output",args);
		if (seqMatrix_output == null) {
            System.out.println("Required argument '-seqMatrix_output' missing");
            return -1;
        }
		
        colsize = null;
		
        //subRowSize: row size of sub matrix As that split from A matrix 
        subRowSize = getArgument("-subRowSize",args);
		
	job1ReduceTasks = getArgument("-job1ReduceTasks",args);
	if (job1ReduceTasks == null) {
            job1ReduceTasks = "1";
        }
				
	MaxInputSplit = getArgument("-mis",args);
	if (MaxInputSplit == null) {
            MaxInputSplit = "64";
        }
		
		
		//get matrix A & B column size
        fs = FileSystem.get(getConf());
		try {
			if(!fs.exists(new Path(inputpath)))
			{
			 System.out.println("Input file dose not exist!");
			 return -1;
			}
			FileStatus[] filestatus = fs.listStatus(new Path(inputpath));
            FSDataInputStream d = new FSDataInputStream(fs.open(filestatus[0].getPath()));
            BufferedReader reader = new BufferedReader(new InputStreamReader(d));
            String line = "";
			line = reader.readLine();
			colsize = Integer.toString(line.split(" ").length);
			reader.close();
		}
		catch(Exception e)
		{
		 e.printStackTrace();
		 return 0;
		}
		
		if(subRowSize==null)
		{
		 subRowSize = colsize;
		}
		
		int subrow_size = Integer.parseInt(subRowSize);		
		
		if(subrow_size<Integer.parseInt(colsize))
		{
		 System.out.println("-subRowSize need to equal or bigger than matrix colsize: "+colsize);
		 return 0;
		}
		
		fileGather fgather = new fileGather(new Path(inputpath),"",fs);
		MaxInputSplit = Integer.toString(Checker.checkMis(Integer.valueOf(MaxInputSplit),fgather.getInputSize(),fs));
		int srs = Checker.checkSrs(Integer.valueOf(subRowSize),Integer.valueOf(colsize),Integer.valueOf(MaxInputSplit));
		subRowSize = Integer.toString(srs);	
		
		//Job 1-1: Create sub matrix number list
		ToolRunner.run(getConf(), new subMatrixCountJob(), new String[]{
              "-input", inputpath,
              "-output", subMatrixCount_output,
              "-subrow_size", subRowSize,
			  "-mis",MaxInputSplit,
			  "-colsize", colsize});
		//Job 1-2: generate sequential sub matrix depend on number_list
		ToolRunner.run(getConf(), new subMatrixGenerateJob(), new String[]{
              "-input", inputpath,
              "-output", seqMatrix_output,
			  "-part_list", subMatrixCount_output+"/part-00000",
              "-reducer_number", job1ReduceTasks,
              "-subrow_size", subRowSize,
			  "-mis",MaxInputSplit,
			  "-colsize", colsize});
        return 0;
    }
	
	private static int printUsage() {
        System.out.println("usage: -input <filepath> -output <outputpath> [-subRowSize <int>]");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }
	
	public String getColSize()
	{
	 return colsize;
	}
    
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new SequenceFileMatrixMaker(), args);
  }
}
