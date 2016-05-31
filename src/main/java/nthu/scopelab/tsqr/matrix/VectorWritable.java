/**
 * A custom serializable object for DenseVector and SparseVector
 */
package nthu.scopelab.tsqr.matrix;

import org.apache.hadoop.io.Writable;
import java.util.Iterator;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.sparse.SparseVector;
import no.uib.cipr.matrix.VectorEntry;

public class VectorWritable implements Writable,Serializable {

    private Vector A;
	private boolean isDense = true;
	
    public VectorWritable() {
        A = null;
    }
	
    public VectorWritable(DenseVector inv) {
		 isDense = true;
         A = inv;
    }
	
	public VectorWritable(SparseVector inv) {
		 isDense = false;
         A = inv;
    }
	
	@Override
    public void write(DataOutput out) throws IOException {
		if(isDense)
		{
		 out.writeByte(0);
		 out.writeInt(A.size());
		 for(int i=0;i<A.size();i++)
			out.writeDouble(A.get(i));
		}
		else
		{
		 SparseVector As = (SparseVector)A;
		 out.writeByte(1);
		 out.writeInt(A.size());
		 out.writeInt(As.getUsed());
		 Iterator<VectorEntry> iter = As.iterator();
		 while(iter.hasNext())
		 {
		  VectorEntry ve = iter.next();
		  out.writeInt(ve.index());
		  out.writeDouble(ve.get());
		 }
		}
    }
	
	@Override
    public void readFields(DataInput in) throws IOException {
		byte Flag = in.readByte();
		int size = in.readInt();
		
		if(Flag==0)
		{
		 isDense = true;
		 A = new DenseVector(size);
		 for(int i=0;i<size;i++)
			A.set(i,in.readDouble());
		}
		else
		{
		 isDense = false;
		 A = new SparseVector(size);
		 int nonzero = in.readInt();
		 for(int i=0;i<nonzero;i++)
		  A.set(in.readInt(),in.readDouble());
		}
    }
	
	public void set(DenseVector inv) {
		 isDense = true;
         A = inv;
    }
	
    public void set(SparseVector inv) {
		 isDense = false;
         A = inv;
    }

    public Vector get() {
        return A;
    }
	
	public boolean isDense()
	{
		return isDense;
	}
}
