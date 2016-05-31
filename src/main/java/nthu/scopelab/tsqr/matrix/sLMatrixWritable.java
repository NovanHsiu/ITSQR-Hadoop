/**
 * For Spark implemenation, Extends from sMatrixWritable, this class provide a long array to store id which is row index in matrix
 */

package nthu.scopelab.tsqr.matrix;

import org.apache.hadoop.io.Writable;
import java.util.Arrays;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class sLMatrixWritable extends sMatrixWritable implements Serializable {

  private long[] longArray;
  private int longLength = 0;
  public sLMatrixWritable() {
   super();
   longArray = null;  
  }
  
  public sLMatrixWritable(cmDenseMatrix mat) {
   super(mat);
  }
  
  public sLMatrixWritable(cmSparseMatrix mat) {
    super(mat);
  }
  
  public sLMatrixWritable(long[] array,cmDenseMatrix mat) {
	super(mat);
    longLength = array.length;
    longArray = array;
  }
  
  public sLMatrixWritable(long[] array,int al,cmDenseMatrix mat) {
	super(mat);
    longLength = al;
    longArray = array;
  }
  
  public sLMatrixWritable(long[] array, cmSparseMatrix mat) {
	super(mat);
    longLength = array.length;
    longArray = array;
  }
  
  public sLMatrixWritable(long[] array,int al,cmSparseMatrix mat) {
	super(mat);
    longLength = al;
    longArray = array;
  }
  
  public void setLMat(long[] array, cmDenseMatrix mat) {
	super.set(mat);
    longLength = array.length;
    longArray = array;
  }
  
  public void setLMat(long[] array, int al,cmDenseMatrix mat) {
	super.set(mat);
    longLength = al;
    longArray = array;
  }
  
  public void setLMat(long[] array, cmSparseMatrix mat) {
	super.set(mat);
    longLength = array.length;
    longArray = array;
  }
  
  public void setLMat(long[] array, int al, cmSparseMatrix mat) {
	super.set(mat);
    longLength = al;
    longArray = array;
  }
  
  public long[] getLongArray() {
    return longArray;
  }
    
  public void setLongArray(long[] array) {
	this.longLength = array.length;
    this.longArray = array;
  }
  
  public void setLongArray(long[] array, int length) {
	this.longLength = length;
    this.longArray = array;
  }
  
   public int getLongLength() {
	return longLength;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(longLength);
	for(int i=0;i<longLength;i++)
	 out.writeLong(longArray[i]);
    super.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    longLength = in.readInt();
	if(longArray==null)
	 longArray = new long[longLength*2];
	else if(longArray.length<longLength)
	 longArray = new long[longLength*2];
	for(int i=0;i<longLength;i++)
	 longArray[i]=in.readLong();
    super.readFields(in);
  }
}
