package nthu.scopelab.tsqr.matrix;

import no.uib.cipr.matrix.UpperTriangDenseMatrix;
/**
 * Present a column major upper triangular matrix data structure for Hadoop serizable file
 */
public class cmUpperTriangDenseMatrix extends UpperTriangDenseMatrix {
	
	double[] data = null;
	
	public cmUpperTriangDenseMatrix(int n) {
	 super(n);
	 data = new double[(n*(n+1))/2];
    }
	
	public cmUpperTriangDenseMatrix(double[] indata) {
	 super((int)Math.round((-1 + Math.sqrt(1 + 8 * indata.length)) / 2));
	 data = indata;
	 int count=0;
	 for(int i=0;i<numRows();i++)
	  for(int j=i;j<numColumns();j++)
	  {
	   set(i,j,indata[count]);
	   count++;
	  }
    }
	
	public double[] getData()
	{
	  int count=0;
	  for(int i=0;i<numRows();i++)
	   for(int j=i;j<numColumns();j++)
	   {
	    data[count] = get(i,j);
	    count++;
	   }
	 return data;
	}
	
}
