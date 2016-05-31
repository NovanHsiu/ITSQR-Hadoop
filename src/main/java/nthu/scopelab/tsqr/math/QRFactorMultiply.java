/**
 *  provide some arithmetic function
**/
package nthu.scopelab.tsqr.math;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.math.BigDecimal;
import org.netlib.util.intW;

import org.netlib.blas.Dtrmm;
import org.netlib.blas.Dgemm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.io.SequenceFile;

import no.uib.cipr.matrix.DenseMatrix;
import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.MatrixWritable;

public class QRFactorMultiply {

	/**
	 * Multiply two desematrix A and B, Result = A*B
	 *
	 * @param Atrans
	 *		A is transpose or not: "T" or "N"
	 * @param Btrans
	 *		B is transpose or not: "T" or "N"
	 * @param A
	 *		A matrix
	 * @param B
	 *		B matrix
	 * @param Result
	 *		Result matrix
	 * @return Result
	 *		Result matrix
	 */
	public static cmDenseMatrix Multiply(String Atrans,String Btrans,cmDenseMatrix A,cmDenseMatrix B,cmDenseMatrix Result)
	{
	 //only Accept DenseMatrix
	 int resultRow = A.numRows(), resultColumn = B.numColumns(), innerSide = A.numColumns();
	 if(Atrans.equals("T"))
	 {
	  resultRow = A.numColumns();
	  innerSide = A.numRows();
	 }
	 if(Btrans.equals("T"))
	 {
	  resultColumn = B.numRows();
	 }
	 if(Result==null)
	  throw new NullPointerException();
	 Dgemm.dgemm(Atrans,Btrans,resultRow,resultColumn,innerSide,1.0,A.getData(),0,A.numRows(),B.getData(),0,B.numRows(),0,Result.getData(),0,resultRow);	 
	 return Result;
	}
	
	/**
	 * Do Rounding for double value which depend on specific scale
	 *
	 * @param v
	 *		value
	 * @param scale
	 *		scale
	 * @return rounding value
	 *		rounding value
	 */
	public static double Dround(double v,int scale)
    {
        if(scale<0)
        {
            throw new IllegalArgumentException("The scale must be a positive integer or zero");
        }
        BigDecimal b = new BigDecimal(Double.toString(v));
        BigDecimal one = new BigDecimal("1");
        return b.divide(one,scale,BigDecimal.ROUND_HALF_UP).doubleValue();
    }
	
	/**
	 * Path Filter
	 */
	public static class MyPathFilter implements PathFilter
	{
	 final String pathstr;
	 public MyPathFilter(String filters)
	 {
	  super();
	  pathstr = filters;
	 }
	 	 
	 @Override
	 public boolean accept(Path path) {
			return path.getName().startsWith(pathstr);
	 };	 
	}
}
