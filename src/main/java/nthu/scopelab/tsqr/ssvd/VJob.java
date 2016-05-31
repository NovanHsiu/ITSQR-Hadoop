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
// modify from mahout-6.0 package org.apache.mahout.math.hadoop.stochasticsvd.QJob
// 2013 Hsiu-Cheng Yu
package nthu.scopelab.tsqr.ssvd;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.Vector;

import nthu.scopelab.tsqr.matrix.LMatrixWritable;
import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.VectorWritable;
import nthu.scopelab.tsqr.TSQRunner.fileGather;
import nthu.scopelab.tsqr.TSQRunner.Checker;

/**
 * Computes V=Bt*Uhat*Sigma^(-1) of SSVD
 * This code use new Hadoop API
 * ---
 * part of Modification:
 * 1. Replaced mahout VectorWritable by LMatrixWritable.
 * 2. Replaced mahout Matrix Multiplication by JBLAS dgemm. (JBLAS dgemm is use in QRFactorMultiply Multiply function)
 * 
 */
public class VJob {
  private static final String OUTPUT_V = "v";
  private static final String PROP_UHAT_PATH = "ssvd.uhat.path";
  private static final String PROP_SIGMA_PATH = "ssvd.sigma.path";
  private static final String PROP_V_HALFSIGMA = "ssvd.v.halfsigma";
  private static final String SUB_ROW_SIZE = "sub.row.size";

  private Job job;

  public void start(Configuration conf, Path inputPathBt, Path inputUHatPath,
      Path inputSigmaPath, Path outputPath, int k, int numReduceTasks, int subRowSize,
      boolean vHalfSigma, int mis) throws ClassNotFoundException, InterruptedException,
      IOException {

    job = new Job(conf);
    job.setJobName("V-job");
    job.setJarByClass(VJob.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(job, inputPathBt);
	
	FileSystem fs = FileSystem.get(job.getConfiguration());
	fileGather fgather = new fileGather(inputPathBt,"",fs);
	mis = Checker.checkMis(mis,fgather.getInputSize(),fs);
	FileInputFormat.setMaxInputSplitSize(job,mis*1024*1024);
	
    FileOutputFormat.setOutputPath(job, outputPath);

    // Warn: tight hadoop integration here:
    job.getConfiguration().set("mapreduce.output.basename", OUTPUT_V);
    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(LMatrixWritable.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LMatrixWritable.class);

    job.setMapperClass(VMapper.class);

    job.getConfiguration().set(PROP_UHAT_PATH, inputUHatPath.toString());
    job.getConfiguration().set(PROP_SIGMA_PATH, inputSigmaPath.toString());
    if (vHalfSigma) {
      job.getConfiguration().set(PROP_V_HALFSIGMA, "y");
    }
    job.getConfiguration().setInt(QJob.PROP_K, k);
	job.getConfiguration().setInt(SUB_ROW_SIZE, subRowSize);
    job.setNumReduceTasks(0);
    job.submit();
	//job.waitForCompletion(true);
  }

  public void waitForCompletion() throws IOException, ClassNotFoundException,
      InterruptedException {
    job.waitForCompletion(false);

    if (!job.isSuccessful()) {
      throw new IOException("V job unsuccessful.");
    }

  }

  public static final class VMapper extends
      Mapper<IntWritable, VectorWritable, LongWritable, LMatrixWritable> {

    private cmDenseMatrix uHat;
    private DenseVector sValues;
    private int kp;
    private int k;
	private int subRowSize;
	private List<DenseVector> curRowBuffer = new ArrayList<DenseVector>();
    private List<Integer> curIdBuffer = new ArrayList<Integer>();
	private long[] longArray = null;
	private cmDenseMatrix submatrix = null;
	private LMatrixWritable ovalue = new LMatrixWritable();
	@Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      super.setup(context);
      FileSystem fs = FileSystem.get(context.getConfiguration());
      Path uHatPath = new Path(context.getConfiguration().get(PROP_UHAT_PATH));

      Path sigmaPath = new Path(context.getConfiguration().get(PROP_SIGMA_PATH));

      uHat = new cmDenseMatrix(SSVDSolver.loadDistributedRowMatrix(fs,
          uHatPath, context.getConfiguration()));
      // since uHat is (k+p) x (k+p)
      kp = uHat.numColumns();
      k = context.getConfiguration().getInt(QJob.PROP_K, kp);
	  subRowSize = context.getConfiguration().getInt(SUB_ROW_SIZE,kp);     

      sValues = new DenseVector(SSVDSolver.loadDistributedRowMatrix(fs,
          sigmaPath, context.getConfiguration())[0], true);	  
      if (context.getConfiguration().get(PROP_V_HALFSIGMA) != null) {
        for (int i = 0; i < k; i++) {
          sValues.set(i, Math.sqrt(sValues.get(i)));
        }
      }

    }
	
    @Override
    protected void map(IntWritable key, VectorWritable value, Context context)
      throws IOException, InterruptedException {
	  
      Vector btRow = value.get();
	  DenseVector vRow = new DenseVector(k);
      for (int i = 0; i < k; i++) {
        vRow.set(i,btRow.dot(new DenseVector(uHat.getColumn(i))) / sValues.get(i));
      }
	  curIdBuffer.add(key.get());
	  curRowBuffer.add(vRow);
	  if(curRowBuffer.size()>=subRowSize*2)
	  {
	    int rowbuffersize = curRowBuffer.size()/2;
		int mn = rowbuffersize*k;
		if(submatrix == null)
		{
		 submatrix = new cmDenseMatrix(new double[mn*2],rowbuffersize,k);
		 longArray = new long[rowbuffersize*2];
		}
		else if(submatrix.getData().length<mn)
		{
		 submatrix = new cmDenseMatrix(new double[mn*2],rowbuffersize,k);
		 longArray = new long[rowbuffersize*2];
		}
		else
		{
		 submatrix.set(submatrix.getData(),rowbuffersize,k);
		}
		
		//set the output value
		for(int i=0;i<rowbuffersize;i++)
		{
		 longArray[i] = (long)curIdBuffer.remove(0).intValue();
		 submatrix.setRow(i,curRowBuffer.remove(0).getData());
		}
		//write context
		ovalue.setLMat(longArray,rowbuffersize,submatrix);
		context.write(new LongWritable(longArray[0]),ovalue);
	  }
      // V inherits original A column(item) labels.
    }
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
	 if(curRowBuffer.size()>0)
	 {
	 int rowbuffersize = curRowBuffer.size();
	 int mn = rowbuffersize*k;
	 if(submatrix == null)
	 {
		submatrix = new cmDenseMatrix(new double[mn*2],rowbuffersize,k);
		longArray = new long[rowbuffersize*2];
	 }
	 else if(submatrix.getData().length<mn)
	 {
		submatrix = new cmDenseMatrix(new double[mn*2],rowbuffersize,k);
		longArray = new long[rowbuffersize*2];
	 }
	 else
	 {
	  submatrix.set(submatrix.getData(),rowbuffersize,k);
	 }

	 //set the output value
	 for(int i=0;i<rowbuffersize;i++)
	 {
	  longArray[i] = (long)curIdBuffer.remove(0).intValue();
	  submatrix.setRow(i,curRowBuffer.remove(0).getData());	  
	 }
	 //write context
	 ovalue.setLMat(longArray,rowbuffersize,submatrix);
	 context.write(new LongWritable(longArray[0]),ovalue);
	 }
	}

  }

}
