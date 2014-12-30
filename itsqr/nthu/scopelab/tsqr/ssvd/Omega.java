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
package nthu.scopelab.tsqr.ssvd;

import java.util.Arrays;
import java.util.Iterator;
import java.io.Serializable;

import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix;
import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.cmSparseMatrix;

/**
 * Simplistic implementation for Omega matrix in Stochastic SVD method
 * ---
 * Part of modfication:
 * 1. Replaced mahout Vector by mtj Vector
 * 2. Remove computeYRow(Vector,Vector) function and add computeY(cmDenseMatrix,Matrix) function
 */
public class Omega implements Serializable{

  private static final double UNIFORM_DIVISOR = Math.pow(2.0, 64);

  private final long seed;
  private final int kp;

  public Omega(long seed, int k, int p) {
    this.seed = seed;
    kp = k + p;
  }

  /**
   * Get omega element at (x,y) uniformly distributed within [-1...1)
   * 
   * @param row
   *          omega row
   * @param column
   *          omega column
   */
  public double getQuick(int row, int column) {
    long hash = murmur64((long) row << Integer.SIZE | column, 8, seed);
    return hash / UNIFORM_DIVISOR;
  }

  /**
   * A version to compute Y as a sparse vector in case of extremely sparse
   * matrices
   * 
   * @param A
   * @param Y
   */
  public void computeY(FlexCompRowMatrix A, cmDenseMatrix Y) {
    for(int i=0;i<A.numRows();i++)
	{
     for (Iterator<VectorEntry> iter = A.getRow(i).iterator(); iter.hasNext();) {
      VectorEntry ve = iter.next();
	  accumDots(ve.index(), ve.get(), i, Y);
    }
	}
  }
  
  public void computeY(cmSparseMatrix A, cmDenseMatrix Y) {
    for(int i=0;i<A.numRows();i++)
	{
     for (Iterator<VectorEntry> iter = A.getRow(i).iterator(); iter.hasNext();) {
      VectorEntry ve = iter.next();
	  accumDots(ve.index(), ve.get(), i, Y);
    }
	}
  }
  
  public void computeY(cmDenseMatrix A, cmDenseMatrix Y) {
	for(int i=0;i<A.numRows();i++)
	{
     for(int ri=0; ri < A.numColumns(); ri++)
	 {
		accumDots(ri, A.get(i,ri), i, Y);
	 }
	}
  }
  
  protected void accumDots(int aIndex, double aElement, int yRowIndex, cmDenseMatrix Y) {
    for (int j = 0; j < kp; j++) 
	{
	 Y.set(yRowIndex,j,Y.get(yRowIndex,j) + getQuick(aIndex, j) * aElement);
	}
  }

  /**
   * Shortened version for data < 8 bytes packed into {@code len} lowest bytes
   * of {@code val}.
   * 
   * @param val
   *          the value
   * @param len
   *          the length of data packed into this many low bytes of {@code val}
   * @param seed
   *          the seed to use
   * @return murmur hash
   */
  public static long murmur64(long val, int len, long seed) {

    // assert len > 0 && len <= 8;
    long m = 0xc6a4a7935bd1e995L;
    long h = seed ^ len * m;

    long k = val;

    k *= m;
    int r = 47;
    k ^= k >>> r;
    k *= m;

    h ^= k;
    h *= m;

    h ^= h >>> r;
    h *= m;
    h ^= h >>> r;
    return h;
  }

  public static long murmur64(byte[] val, int offset, int len, long seed) {

    long m = 0xc6a4a7935bd1e995L;
    int r = 47;
    long h = seed ^ (len * m);

    int lt = len >>> 3;
    for (int i = 0; i < lt; i++, offset += 8) {
      long k = 0;
      for (int j = 0; j < 8; j++) {
        k <<= 8;
        k |= val[offset + j] & 0xff;
      }

      k *= m;
      k ^= k >>> r;
      k *= m;

      h ^= k;
      h *= m;
    }

    if (offset < len) {
      long k = 0;
      while (offset < len) {
        k <<= 8;
        k |= val[offset] & 0xff;
        offset++;
      }
      h ^= k;
      h *= m;
    }

    h ^= h >>> r;
    h *= m;
    h ^= h >>> r;
    return h;

  }

}
