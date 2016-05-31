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
//modify from mahout-6.0 package org.apache.mahout.math.hadoop.stochasticsvd.SSVDSolver
// 2013 Hsiu-Cheng Yu
package nthu.scopelab.tsqr.ssvd;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Date;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.netlib.lapack.Dlarnv;

import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Vector;

import nthu.scopelab.tsqr.QRFirstJob;
import nthu.scopelab.tsqr.matrix.VectorWritable;
import nthu.scopelab.tsqr.matrix.cmUpperTriangDenseMatrix;
import nthu.scopelab.tsqr.math.EigenSolver;

/**
 * Stochastic SVD solver (API class).
 * 
 * Old version notes of implementation details
 * (https://issues.apache.org/jira/browse/MAHOUT-376).
 *-----
 * I modified QJob, BtJob and modified Mahout Writable Class that are used in this ssvd package.
 * Use TSQR method to compute Q, but use same method as mahout to compute B.
 * 1. Replaced Mahout EigenSolverWrapper by JLapack Dgeev
 */
public class SSVDSolver {

  private double[] svalues;
  private boolean computeU = true;
  private boolean computeV = true;
  private String uPath;
  private String vPath;
  private int outerBlockHeight = 30000;
  private int abtBlockHeight = 200000;

  // configured stuff
  private final Configuration conf;
  private final Path[] inputPath;
  private final Path outputPath;
  private final int ablockRows;
  private final int k;
  private final int p;
  private int q;
  private final int reduceTasks;
  private final int subRowSize;
  private final int mis;
  private final String reduceSchedule;
  private int minSplitSize = -1;
  private boolean cUHalfSigma;
  private boolean cVHalfSigma;
  private boolean overwrite;
  private boolean broadcast = true;

  /**
   * create new SSVD solver. Required parameters are passed to constructor to
   * ensure they are set. Optional parameters can be set using setters .
   * <P>
   * 
   * @param conf
   *          hadoop configuration
   * @param inputPath
   *          Input path (should be compatible with DistributedRowMatrix as of
   *          the time of this writing).
   * @param outputPath
   *          Output path containing U, V and singular values vector files.
   * @param ablockRows
   *          The vertical hight of a q-block (bigger value require more memory
   *          in mappers+ perhaps larger {@code minSplitSize} values
   * @param k
   *          desired rank
   * @param p
   *          SSVD oversampling parameter
   * @param reduceTasks
   *          Number of reduce tasks (where applicable)
   * @param subRowSize
   *          Number of rows of submatrix
   * @param reduceSchedule
   *          Reduce Schedule decide number of Mapreduce and Reduce Tasks in QJob
   * @param mis
   *          Max Map Input Split Size
   * @throws IOException
   *           when IO condition occurs.
   */
  public SSVDSolver(Configuration conf,
                    Path[] inputPath,
                    Path outputPath,
                    int ablockRows,
                    int k,
                    int p,
                    int reduceTasks,
		    int subRowSize,
		    String reduceSchedule,
		    int mis) {
    this.conf = conf;
    this.inputPath = inputPath;
    this.outputPath = outputPath;
    this.ablockRows = ablockRows;
    this.k = k;
    this.p = p;
    this.reduceTasks = reduceTasks;
    this.reduceSchedule = reduceSchedule;
    this.subRowSize = subRowSize;
    this.mis = mis;
  }

  public void setcUHalfSigma(boolean cUHat) {
    this.cUHalfSigma = cUHat;
  }

  public void setcVHalfSigma(boolean cVHat) {
    this.cVHalfSigma = cVHat;
  }

  public int getQ() {
    return q;
  }

  /**
   * sets q, amount of additional power iterations to increase precision
   * (0..2!). Defaults to 0.
   * 
   * @param q
   */
  public void setQ(int q) {
    this.q = q;
  }

  /**
   * The setting controlling whether to compute U matrix of low rank SSVD.
   * 
   */
  public void setComputeU(boolean val) {
    computeU = val;
  }

  /**
   * Setting controlling whether to compute V matrix of low-rank SSVD.
   * 
   * @param val
   *          true if we want to output V matrix. Default is true.
   */
  public void setComputeV(boolean val) {
    computeV = val;
  }

  /**
   * Sometimes, if requested A blocks become larger than a split, we may need to
   * use that to ensure at least k+p rows of A get into a split. This is
   * requirement necessary to obtain orthonormalized Q blocks of SSVD.
   * 
   * @param size
   *          the minimum split size to use
   */
  public void setMinSplitSize(int size) {
    minSplitSize = size;
  }

  /**
   * This contains k+p singular values resulted from the solver run.
   * 
   * @return singlular values (largest to smallest)
   */
  public double[] getSingularValues() {
    return svalues;
  }

  /**
   * returns U path (if computation were requested and successful).
   * 
   * @return U output hdfs path, or null if computation was not completed for
   *         whatever reason.
   */
  public String getUPath() {
    return uPath;
  }

  /**
   * return V path ( if computation was requested and successful ) .
   * 
   * @return V output hdfs path, or null if computation was not completed for
   *         whatever reason.
   */
  public String getVPath() {
    return vPath;
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  /**
   * if true, driver to clean output folder first if exists.
   * 
   * @param overwrite
   */
  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  public int getOuterBlockHeight() {
    return outerBlockHeight;
  }

  /**
   * The height of outer blocks during Q'A multiplication. Higher values allow
   * to produce less keys for combining and shuffle and sort therefore somewhat
   * improving running time; but require larger blocks to be formed in RAM (so
   * setting this too high can lead to OOM).
   * 
   * @param outerBlockHeight
   */
  public void setOuterBlockHeight(int outerBlockHeight) {
    this.outerBlockHeight = outerBlockHeight;
  }

  public int getAbtBlockHeight() {
    return abtBlockHeight;
  }

  /**
   * the block height of Y_i during power iterations. It is probably important
   * to set it higher than default 200,000 for extremely sparse inputs and when
   * more ram is available. y_i block height and ABt job would occupy approx.
   * abtBlockHeight x (k+p) x sizeof (double) (as dense).
   * 
   * @param abtBlockHeight
   */
  public void setAbtBlockHeight(int abtBlockHeight) {
    this.abtBlockHeight = abtBlockHeight;
  }

  public boolean isBroadcast() {
    return broadcast;
  }

  /**
   * If this property is true, use DestributedCache mechanism to broadcast some
   * stuff around. May improve efficiency. Default is false.
   * 
   * @param broadcast
   */
  public void setBroadcast(boolean broadcast) {
    this.broadcast = broadcast;
  }

  /**
   * run all SSVD jobs.
   * 
   * @throws IOException
   *           if I/O condition occurs.
   */
  public void run() throws Exception {
    try {
	  System.out.println("SSVD start!");
      FileSystem fs = FileSystem.get(conf);

      Path qPath = new Path(outputPath, "Q-job");
      Path btPath = new Path(outputPath, "Bt-job");
      Path yPath = new Path(outputPath, "Y-job"); //tetst phase
      Path uHatPath = new Path(outputPath, "UHat");
      Path svPath = new Path(outputPath, "Sigma");
      Path uPath = new Path(outputPath, "U");
      Path vPath = new Path(outputPath, "V");

      if (overwrite) {
        fs.delete(outputPath, true);
      }

	  int[] iseed = {0,0,0,1};
	  double[] x = new double[1];
	  Dlarnv.dlarnv(2,iseed,0,1,x,0);
	  long seed = (long)(x[0]*(double)Long.MAX_VALUE);

	  long start, end;
	 		
	start = new Date().getTime();
	QJob.run(conf,
               inputPath,
               qPath.toString(),
		reduceSchedule,
               k,
               p,
               seed,
		mis);
	end = new Date().getTime();
	System.out.println("Q-Job done "+Long.toString(end-start));
	Logger LOG = LoggerFactory.getLogger(SSVDSolver.class);
	 
      /*
       * restrict number of reducers to a reasonable number so we don't have to
       * run too many additions in the frontend when reconstructing BBt for the
       * last B' and BB' computations. The user may not realize that and gives a
       * bit too many (I would be happy i that were ever the case though).
       */
	   
      start = new Date().getTime();
	  BtJob.run(conf,
                inputPath,
				btPath,
				qPath.toString(),
				k,
                p,
                outerBlockHeight,
				q <= 0 ? Math.min(1000, reduceTasks) : reduceTasks,
				q <= 0,
				reduceSchedule,
				mis);

	  end = new Date().getTime();
      System.out.println("Bt-Job done "+Long.toString(end-start));
	  
      // power iterations is unnecessary in application of recommendation system 	  
	  /*for (int i = 0; i < q; i++) {
	    Path btPathGlob = new Path(btPath, BtJob.OUTPUT_BT + "-*");
		Path aBtPath = new Path(outputPath, String.format("ABt-job-%d", i + 1));      
        qPath = new Path(outputPath, String.format("ABtQ-job-%d", i + 1));		
        ABtDenseOutJob.run(conf,
                           inputPath,
                           btPathGlob,
                           aBtPath,//qPath,
                           //ablockRows,
                           //minSplitSize,
                           k,
                           p,
                           //abtBlockHeight,
                           reduceTasks,
                           //broadcast
						   mis);
		
		ToolRunner.run(conf, new QRFirstJob(), new String[]{
              "-input", aBtPath.toString(),
              "-output", qPath.toString(),
			  "-mis",Integer.toString(mis),
			  "-colsize", Integer.toString(k+p),
              "-reduceSchedule", reduceSchedule});
			  
        btPath = new Path(outputPath, String.format("Bt-job-%d", i + 1));

        BtJob.run(conf,
                  inputPath,
				  btPath,
                  qPath.toString(),                 
                  k,
                  p,
                  outerBlockHeight,
                  i == q - 1 ? Math.min(1000, reduceTasks) : reduceTasks,
                  i == q - 1,
				  reduceSchedule,
				  mis);
      }*/
	  
     cmUpperTriangDenseMatrix bbt =
        loadAndSumUpperTriangMatrices(fs, new Path(btPath, BtJob.OUTPUT_BBT
            + "-*"), conf);

      // convert bbt to something our eigensolver could understand
      assert bbt.numColumns() == k + p;

      double[][] bbtSquare = new double[k + p][];
      for (int i = 0; i < k + p; i++) {
        bbtSquare[i] = new double[k + p];
      }

      for (int i = 0; i < k + p; i++) {
        for (int j = i; j < k + p; j++) {
          bbtSquare[i][j] = bbtSquare[j][i] = bbt.get(i, j);
        }
      }

      svalues = new double[k + p];

      // try something else.
      EigenSolver eigenWrapper = new EigenSolver(bbtSquare);
      double[] eigenva2 = eigenWrapper.getWR();
	  
      for (int i = 0; i < k + p; i++) {
        svalues[i] = Math.sqrt(eigenva2[i]); // sqrt?
      }
      // save/redistribute UHat
      double[][] uHat = eigenWrapper.getVL();
	  //double[][] uHat = eigenWrapper.getUHat();
	  
      fs.mkdirs(uHatPath);
      SequenceFile.Writer uHatWriter =
        SequenceFile.createWriter(fs,
                                  conf,
                                  uHatPath = new Path(uHatPath, "uhat.seq"),
                                  IntWritable.class,
                                  VectorWritable.class,
                                  CompressionType.BLOCK);
	  
      int m = uHat.length;
      IntWritable iw = new IntWritable();
      VectorWritable vw = new VectorWritable();
	  
      for (int i = 0; i < m; i++) {
        vw.set(new DenseVector(uHat[i],true));
        iw.set(i);
        uHatWriter.append(iw, vw);
      }
	  uHatWriter.close();

      SequenceFile.Writer svWriter =
        SequenceFile.createWriter(fs,
                                  conf,
                                  svPath = new Path(svPath, "svalues.seq"),
                                  IntWritable.class,
                                  VectorWritable.class,
                                  CompressionType.BLOCK);

      vw.set(new DenseVector(svalues, true));
      svWriter.append(iw, vw);

      svWriter.close();

	  start = new Date().getTime();
      UJob ujob = null;	  
      if (computeU) {
        ujob = new UJob();
		ujob.start(conf,
                   new Path(btPath, BtJob.Q_MAT+ "-*"),
                   uHatPath,
                   svPath,
                   uPath,
                   k,
                   cUHalfSigma,
				   mis);				   
        // actually this is map-only job anyway
      }

      VJob vjob = null;
      if (computeV) {
        vjob = new VJob();
        vjob.start(conf,
                   new Path(btPath, BtJob.OUTPUT_BT + "-*"),
                   uHatPath,
                   svPath,
                   vPath,
                   k,
                   reduceTasks,
				   subRowSize,
                   cVHalfSigma,
				   mis);
      }

      if (ujob != null) {
        ujob.waitForCompletion();
        this.uPath  = uPath.toString();
      }
	  System.out.println("U-Job done ");
	  
      if (vjob != null) {
        vjob.waitForCompletion();
        this.vPath = vPath.toString();
      }
	end = new Date().getTime();
	System.out.println("U-Job+V-Job done "+(end-start));
	
    } catch (InterruptedException exc) {
      throw new IOException("Interrupted", exc);
    } catch (ClassNotFoundException exc) {
      throw new IOException(exc);
    }

  }

  private static final Pattern OUTPUT_FILE_PATTERN =
    Pattern.compile("(\\w+)-(m|r)-(\\d+)(\\.\\w+)?");

  static final Comparator<FileStatus> PARTITION_COMPARATOR =
    new Comparator<FileStatus>() {
      private final Matcher matcher = OUTPUT_FILE_PATTERN.matcher("");

      @Override
      public int compare(FileStatus o1, FileStatus o2) {
        matcher.reset(o1.getPath().getName());
        if (!matcher.matches()) {
          throw new IllegalArgumentException("Unexpected file name, unable to deduce partition #:"
              + o1.getPath());
        }
        int p1 = Integer.parseInt(matcher.group(3));
        matcher.reset(o2.getPath().getName());
        if (!matcher.matches()) {
          throw new IllegalArgumentException("Unexpected file name, unable to deduce partition #:"
              + o2.getPath());
        }

        int p2 = Integer.parseInt(matcher.group(3));
        return p1 - p2;
      }

    };
	
	/**
   * helper capabiltiy to load distributed row matrices into dense matrix (to
   * support tests mainly).
   * 
   * @param fs
   *          filesystem
   * @param glob
   *          FS glob
   * @param conf
   *          configuration
   * @return Dense matrix array
   * @throws IOException
   *           when I/O occurs.
   */
  public static double[][] loadDistributedRowMatrix(FileSystem fs,
                                                    Path glob,
                                                    Configuration conf)
    throws IOException {

    FileStatus[] files = fs.globStatus(glob);
    if (files == null) {
      return null;
    }

    List<double[]> denseData = new ArrayList<double[]>();

    /*
     * assume it is partitioned output, so we need to read them up in order of
     * partitions.
     */
    Arrays.sort(files, PARTITION_COMPARATOR);
	SequenceFile.Reader reader = null;
	IntWritable key = null;
	VectorWritable value = null;
    for (FileStatus fstat : files) {
		reader = new SequenceFile.Reader(fs, fstat.getPath(), fs.getConf());
		try{
		key = (IntWritable) reader.getKeyClass().newInstance();
		value = (VectorWritable) reader.getValueClass().newInstance();
		}
		catch(Exception e)
		{
		 e.printStackTrace();
		}
		while(reader.next(key,value))
		{
        Vector v = value.get();
        int size = v.size();
        double[] row = new double[size];
        for (int i = 0; i < size; i++) {
          row[i] = v.get(i);
        }
        // ignore row label.
        denseData.add(row);
		}
    }
	if(reader!=null)
	 reader.close();
    return denseData.toArray(new double[denseData.size()][]);
  }
  
  /**
   * Load multiplel upper triangular matrices and sum them up.
   * 
   * @param fs
   * @param glob
   * @param conf
   * @return the sum of upper triangular inputs.
   * @throws IOException
   */
  public static cmUpperTriangDenseMatrix
      loadAndSumUpperTriangMatrices(FileSystem fs,
                                        Path glob,
                                        Configuration conf) throws IOException {

    FileStatus[] files = fs.globStatus(glob);
    if (files == null) {
      return null;
    }
    /*
     * assume it is partitioned output, so we need to read them up in order of
     * partitions.
     */
    Arrays.sort(files, PARTITION_COMPARATOR);
    DenseVector result = null;
	SequenceFile.Reader reader = null;
	Writable rkey;
	IntWritable key = null;
	VectorWritable value = null;
    for (FileStatus fstat : files) {
		reader = new SequenceFile.Reader(fs,fstat.getPath(),fs.getConf());
		try{
		key = (IntWritable) reader.getKeyClass().newInstance();
		value = (VectorWritable) reader.getValueClass().newInstance();
		}
		catch(Exception e)
		{
		 e.printStackTrace();
		}
		while(reader.next(key,value))
		{
        Vector v = value.get();
        if (result == null) {
          result = new DenseVector(v);
        } else {
          result.add(v);
        }
		}
    }
	if(reader!=null)
	 reader.close();
    if (result == null) {
      throw new IOException("Unexpected underrun in upper triangular matrix files");
    }
    return new cmUpperTriangDenseMatrix(result.getData());
  }
}
