/** 
 * For Spark implemenation
**/
package nthu.scopelab.tsqr.matrix;

import org.apache.hadoop.io.Writable;
import java.util.Iterator;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import no.uib.cipr.matrix.sparse.SparseVector;
import no.uib.cipr.matrix.VectorEntry;

/**
 * A custom serializable object for cmDenseMatrix and cmSparseMatrix
 */
public class sMatrixWritable implements Writable,Serializable {

    private cmDenseMatrix denseA;
	private cmSparseMatrix sparseA;
	protected int m, n;
	private int TaskID = 0;
	private boolean isDense = true;
	
    public sMatrixWritable() {
        denseA = null;
		sparseA = null;
    }
		
	public sMatrixWritable(cmDenseMatrix inmat) {
		 isDense = true;
         denseA = inmat;
		 m = inmat.numRows();
		 n = inmat.numColumns();
    }
	
	public sMatrixWritable(cmSparseMatrix inmat) {
		 isDense = false;
         sparseA = inmat;
		 m = sparseA.numRows();
		 n = sparseA.numRows();
    }
	
	@Override
    public void write(DataOutput out) throws IOException {
		if(isDense)
		{
		 out.writeByte(1);
		 out.writeInt(m);
		 out.writeInt(n);
		 int mn = m*n;
		 for(int i=0;i<mn;i++)
		  out.writeDouble(denseA.getData()[i]);
		 out.writeInt(TaskID);
		}
		else
		{
		 out.writeByte(0);
		 out.writeInt(sparseA.numRows());
		 out.writeInt(sparseA.numColumns());
		 int nzRowSize = 0;
		 for(int i=0;i<sparseA.numRows();i++)
		 {
		  SparseVector sv = (SparseVector) sparseA.getRow(i);
		  nzRowSize = sv.getUsed();
		  out.writeInt(nzRowSize);
		  for(VectorEntry ve:sv)
		  {
		   out.writeInt(ve.index());
		   out.writeDouble(ve.get());
		  }
		 }
		 out.writeInt(TaskID);
		}
    }
	
	@Override
    public void readFields(DataInput in) throws IOException {
		byte Flag = in.readByte();
		int rownz;
		m = in.readInt();
		n = in.readInt();
		int mn = m*n;
		if(Flag==1)
		{
		 isDense = true;
		 if(denseA==null)
		  denseA = new cmDenseMatrix(new double[2*mn],m,n);
		 else
		 {
		  if(denseA.getData().length<mn)
		   denseA = new cmDenseMatrix(new double[2*mn],m,n);
		  else
		   denseA = new cmDenseMatrix(denseA.getData(),m,n);
		 }
		 for(int i=0;i<mn;i++)
		  denseA.getData()[i] = in.readDouble();
		 TaskID = in.readInt();
		}
		else
		{
		 isDense = false;
		 sparseA = new cmSparseMatrix(m,n);
		 		
		 //rowPointers.length is equal to Matrix.numRows	 
		 for(int i=0;i<m;i++)
		 {
		  rownz = in.readInt();
		  for(int j=0;j<rownz;j++)
		  {
		   sparseA.set(i,in.readInt(),in.readDouble());
		  }
		 }
		 TaskID = in.readInt();
		}
    }
		
	public void set(cmDenseMatrix inmat) {
		 isDense = true;
         denseA = inmat;
		 m = inmat.numRows();
		 n = inmat.numColumns();
    }
		
    public void set(cmSparseMatrix inmat) {
		 isDense = false;
         sparseA = inmat;
    }

    public cmDenseMatrix getDense() {
        return denseA;
    }
	
	public cmSparseMatrix getSparse() {
        return sparseA;
    }
	
	public boolean isDense()
	{
		return isDense;
	}
	
	public void setTaskID(int taskid) {
		TaskID =  taskid;
    }
	
	public int getTaskID()
	{
	 return TaskID;
	}
	
	public int matNumRows()
	{
	 return m;
	}
	
	public int matNumColumns()
	{
	 return n;
	}
}
