package nthu.scopelab.tsqr.rs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.FileStatus;

import nthu.scopelab.tsqr.TSQRunner;
import nthu.scopelab.tsqr.matrix.sMatrixWritable;
import nthu.scopelab.tsqr.matrix.cmDenseMatrix;

/**
 * This code could turn the matrix from Hadoop sequencefile into text file and then print out
 *
 * Example Usage:
 
	${HADOOP_HOME}/bin/hadoop jar ${TSQR_PACKAGE}/TSQR.jar nthu.scopelab.tsqr.rs.readSFsMat -input tsqrOutput/part-00000
   
 */ 
public class readSFsMat extends TSQRunner {
  @Override
  public int run(String[] args) throws Exception {
        if (args.length == 0) {
            return printUsage();
        }
                   
        String inputpath = getArgument("-input",args);
        if (inputpath == null) {
            System.out.println("Required argument '-input' missing");
            return -1;
        }
        
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fstat = fs.listStatus(new Path(inputpath));
		
		for(int fsi=0;fsi<fstat.length;fsi++)
		{
		
		SequenceFile.Reader sreader = new SequenceFile.Reader(fs, fstat[fsi].getPath(), conf);
		try {
		if(sreader.getKeyClass().isInstance(new IntWritable()))
		{
		IntWritable rkey = (IntWritable) sreader.getKeyClass().newInstance();
		sMatrixWritable rvalue = (sMatrixWritable) sreader.getValueClass().newInstance();
		 while (sreader.next(rkey, rvalue))
		 {
		  System.out.println("key: "+rkey.get());
		  cmDenseMatrix dmat = (cmDenseMatrix) rvalue.getDense();
		  System.out.println("m "+dmat.numRows()+", n "+dmat.numColumns());
		  if(dmat.numRows()>0 && dmat.numColumns()>0)
		  {
		  for(int i=0;i<dmat.numRows();i++)
		  {
		   for(int j=0;j<dmat.numColumns();j++)
		    System.out.print(dmat.get(i,j)+" ");
		   System.out.println();
		  }
		  }
		 }
		}
		else
		{
		LongWritable rkey = (LongWritable) sreader.getKeyClass().newInstance();
		sMatrixWritable rvalue = (sMatrixWritable) sreader.getValueClass().newInstance();
		 while (sreader.next(rkey, rvalue))
		 {
		  System.out.println("key: "+rkey.get());
		  cmDenseMatrix dmat = (cmDenseMatrix) rvalue.getDense();
		  System.out.println("m "+dmat.numRows()+", n "+dmat.numColumns());
		  if(dmat.numRows()>0 && dmat.numColumns()>0)
		  {
		  for(int i=0;i<dmat.numRows();i++)
		  {
		   for(int j=0;j<dmat.numColumns();j++)
		    System.out.print(dmat.get(i,j)+" ");
		   System.out.println();
		  }
		  }
		 }
		}
		 sreader.close();
		 }
		 catch(Exception e)
		 {
		  e.printStackTrace();
		 }
		 
		}//for fstat.length
		return 0;
    }
	
	private static int printUsage() {
        System.out.println("usage: -input <filepath>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }
    
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new readSFsMat(), args);
  }
}
