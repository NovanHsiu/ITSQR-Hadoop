/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
/*
* Calculates the eigen decomposition
*/

//modify from mahout-6.0 package org.apache.mahout.math.ssvd.EigenSolverWrapper.java
// 2013 Hsiu-Cheng Yu
package nthu.scopelab.tsqr.math;

import org.netlib.lapack.Dgeev;
import org.netlib.lapack.Dgesvd;

import org.netlib.util.intW;

public class EigenSolver
{
    final int n;
	double[] D;
    double[] wr, wi, work;
    double[] vl, vr;
	/**
	 * Initialize all members
	 *
	 * @param A
	 *		A matrix in 2D double array
	 */
    public EigenSolver(double[][] A)
    {
	  if(A.length!=A[0].length)
	   throw new IllegalArgumentException("row size:"+A.length+"!="+A[0].length+"column size");
	  n = A.length;
	  wr = new double[n]; wi = new double[n]; 
	  vl = new double[n*n]; vr = new double[n*n];
	  work = new double[4*n];
	  intW info = new intW(0);
	  D = trun2Dto1D(A);
	  Dgeev.dgeev("V","V",n,D,0,n,wr,0,wi,0,vl,0,n,vr,0,n,work,0,4*n,info);
	  /*int lwork = 5*n;
	  work = new double[5*n];
	  Dgesvd.dgesvd("S","S",n,n,D,0,n,wr,0,vl,0,n,vr,0,n,work,0,lwork,info);*/
    }

	public double[] getWR()
    {		
        return wr;
    }
	
	public double[] getWI()
    {		
        return wi;
    }
	
	public double[][] getVL()
    {		
        return trun1Dto2D(vl);
    }
	
	public double[][] getVR()
    {	
        return trun1Dto2D(vr);
    }
		
	public double[][] getD()
	{		
		return trun1Dto2D(D);
	}
	
	public int getDim()
	{
	 return n;
	}
	
	private double[] trun2Dto1D(double[][] D2)
	{
	 double[] D1 = new double[n*n];
	 for(int i=0;i<n;i++)
	   for(int j=0;j<n;j++)
	    D1[i*n+j] = D2[j][i];
	 return D1;
	}
	
	private double[][] trun1Dto2D(double[] D1)
	{
	 double[][] D2 = new double[n][n];
	 for(int i=0;i<n;i++)
	   for(int j=0;j<n;j++)
	    D2[j][i] = D1[i*n+j];
	 return D2;
	}
}
