package nthu.scopelab.tsqr.matrix;

import java.util.Arrays;
import java.io.Serializable;

import no.uib.cipr.matrix.sparse.SparseVector;
/**
 * Present a column major dense matrix data structure for Hadoop serizable file
 * This class could modify the row size and column size without reallocated memory
 */
public class cmSparseMatrix implements Serializable{
	
	private int m, n;
    private SparseVector[] data;	
	
    public cmSparseMatrix() {
        data = null;
    }
		
	public cmSparseMatrix(int im,int in) {        
		 m = im;
		 n = in;
		 data = new SparseVector[m];
		 for(int i=0;i<m;i++)
		  data[i] = new SparseVector(n);
    }
	
	public int numRows()
	{
	 return m;
	}
	
	public int numColumns()
	{
	 return n;
	}
	
	public SparseVector[] getData()
	{
	 return data;
	}
	
	public void set(int i,int j,double value)
	{
	 data[i].set(j,value);
	}
	
	public double get(int i,int j)
	{
	 return data[i].get(j);
	}
		
	/*public cmSparseMatrix copy()
	{
	 double ndata[] = Arrays.copyOf(data,m*n);
	 int im = m;
	 int in = n;
	 return new cmSparseMatrix(ndata,im,in);
	}*/
	
	public SparseVector getRow(int i)
	{
		return data[i];
	}
		
	public void setRow(int i,SparseVector row)
	{
		data[i] = row;
	}
	
	public cmSparseMatrix copy()
	{
	 cmSparseMatrix newmat = new cmSparseMatrix(numRows(),numColumns());
	 for(int i=0;i<numRows();i++)
	  newmat.setRow(i,getRow(i).copy());
	 return newmat;
	}
	/*public void zero()
	{
	 Arrays.fill(data,0,m*n,0.0);
	}*/


	/*@Override
	public String toString(){
	 String matstr = "";
	 for(int i=0;i<m;i++)
	 {
	 for(int j=0;j<n;j++)
	 {
	  matstr = matstr + Double.toString(get(i,j)) + " ";
	 }
	 matstr = matstr+"\n";
	 }
	 return matstr;
	}*/
}
