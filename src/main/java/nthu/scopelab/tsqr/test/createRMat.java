package nthu.scopelab.tsqr.test;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.fs.FileStatus;

import nthu.scopelab.tsqr.TSQRunner;
import nthu.scopelab.tsqr.matrix.LMatrixWritable;
import nthu.scopelab.tsqr.matrix.cmDenseMatrix;

/**
 Create mutiple upper triangular dense matrix R  
 */ 
public class createRMat extends TSQRunner {
  @Override
  public int run(String[] args) throws Exception {
        if (args.length == 0) {
            return printUsage();
        }
        
		String output_str = getArgument("-output",args);
        if (output_str == null) {
            System.out.println("Required argument '-output' missing");
            return -1;
        }
		
        String size_str = getArgument("-size",args);
        if (size_str == null) {
            System.out.println("Required argument '-size' missing");
            return -1;
        }
        String number_str = getArgument("-number",args);
        if (number_str == null) {
            System.out.println("Required argument '-number' missing");
            return -1;
        }
		int size = Integer.valueOf(size_str);
		int num = Integer.valueOf(number_str);
		
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		
		SequenceFile.Writer swriter = new SequenceFile.Writer(fs, conf, new Path(output_str), IntWritable.class, LMatrixWritable.class);
		cmDenseMatrix dmat = new cmDenseMatrix(size,size);
		Random rnd = new Random();
		for(int i=0;i<size;i++)
		 for(int j=i;j<size;j++)
		  dmat.set(i,j,rnd.nextDouble());
		IntWritable okey = new IntWritable();
		LMatrixWritable ovalue = new LMatrixWritable(dmat);
		
		try {
		 for(int i=0;i<num;i++)
		 {
		  okey.set(i);
		  swriter.append(okey,ovalue);
		 }
		 swriter.close();
		 }
		 catch(Exception e)
		 {
		  e.printStackTrace();
		 }

		return 0;
    }
	
	private static int printUsage() {
        System.out.println("usage: -size <size of column and row of matrix> -number <number of matrices> -output <output name>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }
    
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new createRMat(), args);
  }
}
