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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.Vector;

import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.VectorWritable;
import nthu.scopelab.tsqr.matrix.LMatrixWritable;
import nthu.scopelab.tsqr.math.QRFactorMultiply;
import nthu.scopelab.tsqr.TSQRunner.fileGather;
import nthu.scopelab.tsqr.TSQRunner.Checker;

/**
 * Computes U=Q*Uhat of SSVD (optionally adding x pow(Sigma, 0.5) )
 * ---
 * part of Modification:
 * 1. Replaced mahout VectorWritable by LMatrixWritable.
 * 2. Replaced mahout Matrix Multiplication by JBLAS dgemm. (JBLAS dgemm is use in QRFactorMultiply Multiply function)
 * 
 */
public class UJob {
  private static final String OUTPUT_U = "u";
  private static final String PROP_UHAT_PATH = "ssvd.uhat.path";
  private static final String PROP_SIGMA_PATH = "ssvd.sigma.path";
  private static final String PROP_U_HALFSIGMA = "ssvd.u.halfsigma";
  
  private JobID jobid;
  private JobClient jobclient;
  public void start(Configuration conf, Path inputPathQ, Path inputUHatPath,
      Path sigmaPath, Path outputPath, int k, boolean uHalfSigma, int mis)
    throws ClassNotFoundException, InterruptedException, IOException {
	String input = "";
	
	JobConf job = new JobConf(conf, UJob.class);
	jobclient = new JobClient(job);
	job.setJobName("UJob");
	job.setInputFormat(SequenceFileInputFormat.class);
	job.setOutputFormat(SequenceFileOutputFormat.class);
	
	job.setMapperClass(MultiplyMapper.class);
	
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LMatrixWritable.class);
	job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(LMatrixWritable.class);
	
	FileSystem fs = FileSystem.get(job);
	fileGather fgather = new fileGather(new Path(inputPathQ.toString().substring(0,inputPathQ.toString().lastIndexOf("/")-1)),"Q-",fs);
	mis = Checker.checkMis(mis,fgather.getInputSize(),fs);
	job.setNumMapTasks(fgather.recNumMapTasks(mis));
	
    job.setNumReduceTasks(0);
	job.set("mapreduce.output.basename", OUTPUT_U);	
	job.set(PROP_UHAT_PATH, inputUHatPath.toString());
	job.set(PROP_SIGMA_PATH, sigmaPath.toString());
	if (uHalfSigma) {
		job.set(PROP_U_HALFSIGMA, "y");
	}
	job.setInt(QJob.PROP_K, k);
	FileSystem.get(job).delete(outputPath, true);	
	FileOutputFormat.setOutputPath(job, outputPath);
	FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
	SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
	
	FileInputFormat.setInputPaths(job, inputPathQ);
	//JobClient.runJob(job);
    jobid = jobclient.submitJob(job).getID();

  }
  
   public void waitForCompletion() throws IOException, ClassNotFoundException,
      InterruptedException {
    jobclient.getJob(jobid).waitForCompletion();
    if (!jobclient.getJob(jobid).isSuccessful()) {
      throw new IOException("U job unsuccessful.");
    }
  }
  
  public static class MultiplyMapper //only work on iteration 1 (index 0)
        extends MapReduceBase implements Mapper<IntWritable, LMatrixWritable, LongWritable, LMatrixWritable>{
	protected FileStatus[] basicv;
	protected FileSystem fs;
    private DenseVector uRow;
    private VectorWritable uRowWritable;
    private int kp;
    private int k, vm ,vn,hm,hn;
    private Vector sValues;
	private final LongWritable okey = new LongWritable();
	private cmDenseMatrix Umat = null;
	private cmDenseMatrix uHat;
	private LMatrixWritable ovalue = new LMatrixWritable();
	
    @Override
    public void map(IntWritable key, LMatrixWritable value, OutputCollector<LongWritable, LMatrixWritable> output, Reporter reporter) 
	 throws IOException {
			//do sub U = Q*Uh matrix multiplication
			vm = value.getDense().numRows();
			vn = value.getDense().numColumns();
			hn = uHat.numColumns();
			hm = uHat.numRows();
			
			if(Umat == null)
			 Umat = new cmDenseMatrix(new double[vm*hn*2],vm,hn);
			else if(Umat.getData().length<vm*hn)
			 Umat = new cmDenseMatrix(new double[vm*hn*2],vm,hn);
			else
			 Umat.set(Umat.getData(),vm,hn);
			 
			Umat = QRFactorMultiply.Multiply("N","N",value.getDense(),uHat,Umat);
			
			ovalue.setLMat(value.getLongArray(),Umat);			
			okey.set((long)key.get());
			output.collect(okey,ovalue);	
    }

     @Override
	 public void configure(JobConf job){
	  try{
	  fs = FileSystem.get(job);	
	  	  
      Path uHatPath = new Path(job.get(PROP_UHAT_PATH));
      Path sigmaPath = new Path(job.get(PROP_SIGMA_PATH));

	  uHat = new cmDenseMatrix(SSVDSolver.loadDistributedRowMatrix(fs,uHatPath, job));
      // since uHat is (k+p) x (k+p)
      kp = uHat.numColumns();
      k = job.getInt(QJob.PROP_K, kp);
	   
	  if(k!=kp)
	  {
	   cmDenseMatrix pre_uHat = uHat;
	   uHat = new cmDenseMatrix(new double[uHat.numRows()*k],uHat.numRows(),k);
	   for(int i=0;i<uHat.numRows();i++)
	   {
	    for(int j=0;j<uHat.numColumns();j++)
		{
		 uHat.set(i,j,pre_uHat.get(i,j));
		}
	   }
	  }

      uRow = new DenseVector(k);
      uRowWritable = new VectorWritable(uRow);

      if (job.get(PROP_U_HALFSIGMA) != null) {
        sValues = new DenseVector(SSVDSolver.loadDistributedRowMatrix(fs,
            sigmaPath, job)[0], true);
        for (int i = 0; i < k; i++) {
          sValues.set(i, Math.sqrt(sValues.get(i)));
        }
      }
	  
	  if (sValues != null)
	  {
		for(int i=0;i<uHat.numRows();i++)
	     for(int j=0;j<uHat.numColumns();j++)
		  uHat.set(i,j,uHat.get(i,j)*sValues.get(j));
	  }
	  
	  
    }//try
	catch(Exception e)
	{
	 e.printStackTrace();
	 throw new NullPointerException("error!");
	}
	}
  }

}
