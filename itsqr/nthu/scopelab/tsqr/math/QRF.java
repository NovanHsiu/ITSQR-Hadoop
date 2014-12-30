/*
* This class calculate QR factorization of A and then store Q and R
*/
package nthu.scopelab.tsqr.math;

import nthu.scopelab.tsqr.matrix.cmDenseMatrix;

import java.util.Iterator;
import org.netlib.lapack.Dgeqrf;
import org.netlib.lapack.Dorgqr;
import org.netlib.util.intW;

public class QRF
{
	private cmDenseMatrix R;
    final int m;
    final int n;
    final int k;
	cmDenseMatrix Q;
    double work[];
    double workGen[];
    final double tau[];
	/**
	 * Initialize all members
	 *
	 * @param m
	 *		row size of matrix
	 * @param n
	 *		column size of matrix
	 */
    public QRF(int m, int n)
    {
		this.m = m;
        this.n = n;
        k = Math.min(m, n);
        tau = new double[k];
		R = new cmDenseMatrix(n,n);
		
        if(n > m)
            throw new IllegalArgumentException("n > m"+", n:"+Integer.toString(n)+", m:"+Integer.toString(m));
        work = new double[1];
        intW info = new intW(0);
        Dgeqrf.dgeqrf(m, n, new double[0],0, m, new double[0],0, work,0, -1, info);
        int lwork;
        if(info.val != 0)
            lwork = n;
        else
            lwork = (int)work[0];
        lwork = Math.max(1, lwork);
        work = new double[lwork];
    }
	
	/**
	 * A methoed to compute QR factorization of matrix A
	 *
	 * @param A
	 *		dense matrix A
	 * @return the QRF object, the members of QRF has factor of QR factorization of m by n matrix A
	 * 
	 */
    public static QRF factorize(cmDenseMatrix A)
    {
        return (new QRF(A.numRows(),A.numColumns())).factor(A.getData());
    }
	
	/**
	 * Compute QR factorization of matrix A
	 *
	 * @param A
	 *		dense matrix A sotre in the double array type (A is column major matrix)
	 * @return QRF object
	 * 
	 */
    public QRF factor(double[] A)
    {      
        intW info = new intW(0);
        Dgeqrf.dgeqrf(m, n, A,0, m, tau,0, work,0, work.length, info);
        if(info.val < 0)
            throw new IllegalArgumentException();		
		//set R
		for(int i=0;i<n;i++)
		 for(int j=0;j<n;j++)
		 {
		   if(i<=j)
		    R.set(i,j,A[j*m+i]);
		   else
		    R.set(i,j,0);
		 }
		 
		//compute Q
		Dorgqr.dorgqr(m,n,n,A,0,m,tau,0,work,0,work.length,info);
		
		//set Q
		Q = new cmDenseMatrix(A,m,n);
		 
      return this;
    }
				
	public cmDenseMatrix getQ()
    {		
        return Q;
    }
	
	public cmDenseMatrix getR()
    {		
        return R;
    }
		
	public int getRowsize()
	{
	 return m;
	}
	public int getColsize()
	{
	 return n;
	}
}
