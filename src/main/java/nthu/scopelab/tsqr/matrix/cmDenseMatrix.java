package nthu.scopelab.tsqr.matrix;

import java.util.Arrays;
import java.io.Serializable;
/**
 * Present a column major dense matrix data structure for Hadoop serizable file
 * This class could modify the row size and column size without reallocated memory
 */
public class cmDenseMatrix implements Serializable{

    private double[] data;
	private int m, n;
	
    public cmDenseMatrix() {
        data = null;
    }
	
	public cmDenseMatrix(double[][] inmat) {
		m = inmat.length;
		n = inmat[0].length;
        data = new double[m*n];
		for(int i=0;i<m;i++)
		 for(int j=0;j<n;j++)
		  data[i*n+j] = inmat[j][i];
    }
	
	public cmDenseMatrix(double[] inmat,int im,int in) {
         data = inmat;
		 m = im;
		 n = in;
    }
	
	public cmDenseMatrix(int im,int in) {        
		 m = im;
		 n = in;
		 data = new double[m*n];
    }
	
	public void set(double[] inmat,int im,int in) {
         data = inmat;
		 m = im;
		 n = in;
    }
	
	public int numRows()
	{
	 return m;
	}
	
	public int numColumns()
	{
	 return n;
	}
	
	public double[] getData()
	{
	 return data;
	}
	
	public void set(int i,int j,double value)
	{
	 data[j*m+i] = value;
	}
	
	public double get(int i,int j)
	{
	 return data[j*m+i];
	}
	
	public cmDenseMatrix add(cmDenseMatrix otherMat)
	{
	 if(otherMat.numRows()!=m || otherMat.numColumns()!=n)
	  throw new IndexOutOfBoundsException("Size of these two matricies can not match!");
	 int mn = m*n;
	 for(int i=0;i<mn;i++)
	  data[i]+=otherMat.getData()[i];
	 return this;
	}
	
	public cmDenseMatrix copy()
	{
	 double ndata[] = Arrays.copyOf(data,m*n);
	 int im = m;
	 int in = n;
	 return new cmDenseMatrix(ndata,im,in);
	}
	
	public double[] getRow(int i)
	{
		double[] row = new double[n];
		for(int j=0;j<row.length;j++)
		 row[j] = this.get(i,j);
		return row;
	}
	
	public double[] getColumn(int i)
	{
		double[] col = new double[m];
		for(int j=0;j<col.length;j++)
		 col[j] = this.get(j,i);
		return col;
	}
	
	public void setRow(int i,double[] row)
	{
		if(row.length!=n)
		 throw new IndexOutOfBoundsException("Can not match! Vector length is "+row.length+"and column size is "+n);
		for(int j=0;j<row.length;j++)
		 this.set(i,j,row[j]);
	}
	
	public void zero()
	{
	 Arrays.fill(data,0,m*n,0.0);
	}


	@Override
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
	}
}
