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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

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
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.DenseVector;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix;

import nthu.scopelab.tsqr.QRFirstJob;
import nthu.scopelab.tsqr.math.QRFactorMultiply;
import nthu.scopelab.tsqr.matrix.LMatrixWritable;
import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.TSQRunner.fileGather;
import nthu.scopelab.tsqr.TSQRunner.Checker;

/**
 * Compute first level of Q.
 * ---
 * part of Modification:
 * 1. Replaced mahout VectorWritable by LMatrixWritable.
 * 2. Extends QRFirstJob in tsqr package to doing factorization.
 * 
 */

@SuppressWarnings("deprecation")
public final class QJob{

  public enum QTime { computation, communication }
  
  public static final String PROP_OMEGA_SEED = "ssvd.omegaseed";
  public static final String PROP_K = "prop.k";
  public static final String PROP_P = "prop.p";
  public static final String QF_MAT = QRFirstJob.QF_MAT;
  
  final static Logger LOG = LoggerFactory.getLogger(QJob.class);

  public static class QRJob extends QRFirstJob.MergeJob
  {	
    public void collect(IntWritable key, LMatrixWritable value,OutputCollector<IntWritable, LMatrixWritable> output) throws IOException
    {
		this.output = output;
		super.collect(key,value);
    }
  }
  
  public static class QMapper
      extends QRJob implements Mapper<IntWritable, LMatrixWritable, IntWritable, LMatrixWritable>{
	  
		private Omega omega;
		private Reporter reporter;
		private int kp;
		private cmDenseMatrix subY = null;
		private LMatrixWritable ovalue;	
		private long qt1, qt2;
		
		@Override
		public void configure(JobConf job){
		 qt1 = new Date().getTime();
		 int k = Integer.parseInt(job.get(PROP_K));
		 int p = Integer.parseInt(job.get(PROP_P));
		 kp = k + p;
		 long omegaSeed = Long.parseLong(job.get(PROP_OMEGA_SEED));
		 omega = new Omega(omegaSeed, k, p);
		 ovalue = new LMatrixWritable();
		 super.configure(job);
        }
		
    public void map(IntWritable key, LMatrixWritable value, OutputCollector<IntWritable, LMatrixWritable> output, Reporter reporter)
      throws IOException{
	  this.reporter = reporter;	    
	  FlexCompRowMatrix subAs = null;
	  cmDenseMatrix subAd = null;
	  int subANumRows = -1;
	  if(!value.isDense())
	  {
		subAs = value.getSparse();
		subANumRows = subAs.numRows();
	  }
	  else
	  {
	    subAd = value.getDense();
		subANumRows = subAd.numRows();
	  }
	  
	  if(subY==null)
	   subY = new cmDenseMatrix(new double[subANumRows*kp*2],subANumRows,kp);
	  else if(subY.getData().length<subANumRows*kp)
	   subY = new cmDenseMatrix(new double[subANumRows*kp*2],subANumRows,kp);
	  else
	   subY.set(subY.getData(),subANumRows,kp);
	   
	  //get the Y sub matrix  from A sub matrix * omega
	  //computeY
	   subY.zero();
	   if(!value.isDense())
	   {
	    omega.computeY(subAs,subY);
	   }
	   else
	   {
	    omega.computeY(subAd,subY);
	   }
	  ovalue.setLMat(value.getLongArray(),subY);
	  t2 = new Date().getTime();
	  super.collect(key,ovalue,output);	  
    }
	
	@Override
	public void close() throws IOException{ 
	  super.close();
	  qt2 = new Date().getTime(); 
	  long totalTime = (qt2-qt1);
	  long outputTime = (mosoutputTime+moscloseTime);
	  System.out.println("Compute Time; "+(totalTime-outputTime));
	  System.out.println("Total Time; "+totalTime);
    }
  }
  
	public static class QReducer 
        extends QRJob implements Reducer<IntWritable, LMatrixWritable, IntWritable, LMatrixWritable>{
		
		private long qt1, qt2;
		
		@Override
		public void configure(JobConf job){
		 qt1 = new Date().getTime();
		 super.configure(job);
		}
				
        public void reduce(IntWritable key, Iterator<LMatrixWritable> values, OutputCollector<IntWritable, LMatrixWritable> output, Reporter reporter)
            throws IOException {
			      this.output = output;
            while (values.hasNext()) 
            {
			     collect(key,values.next());
            }
        }
		
		@Override
		public void close() throws IOException{ 
		super.close();
		qt2 = new Date().getTime();
		long totalTime = (qt2-qt1);
		long outputTime = (mosoutputTime+moscloseTime);
		System.out.println("Compute Time; "+(totalTime-outputTime));
		System.out.println("Total Time; "+totalTime);
		}
		
    }
  
  public static void run(Configuration conf,
                         Path[] inputPaths,
                         String outputPath,
						 String reduceSchedule,
                         int k,
                         int p,
                         long seed,
						 int mis) throws ClassNotFoundException,
    InterruptedException, IOException {
	
    String stages[] = reduceSchedule.split(",");
	String rinput = "";
    String routput = outputPath+"/iter-r-";
	
	for(int i=0;i<stages.length;i++)
	{
	String thenumber = Integer.toString(i+1);
	JobConf job = new JobConf(conf, QJob.class);
	job.setJobName("Q-job-"+thenumber);
	job.setInputFormat(SequenceFileInputFormat.class);
	job.setOutputFormat(SequenceFileOutputFormat.class);
	
	if(i==0)
		job.setMapperClass(QMapper.class);
	else
		job.setMapperClass(IdentityMapper.class);
		
	job.setReducerClass(QReducer.class); 
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(LMatrixWritable.class);
	
	FileSystem fs = FileSystem.get(job);
	Path Paths[];
	fileGather fgather = null;
	if(i==0)
	 fgather = new fileGather(inputPaths,"part",fs);
	else
	 fgather = new fileGather(new Path(rinput),"part",fs);
	Paths = fgather.getPaths();
	mis = Checker.checkMis(mis,fgather.getInputSize(),fs);
	job.setNumMapTasks(fgather.recNumMapTasks(mis));
	
    job.setNumReduceTasks(Integer.parseInt(stages[i]));
	
	job.setInt(QRFirstJob.COLUMN_SIZE,k+p);
	job.setLong(PROP_OMEGA_SEED, seed);
    job.setInt(PROP_K, k);
    job.setInt(PROP_P, p);
	
    fs.delete(new Path(routput+thenumber), true);	
	
    FileInputFormat.setInputPaths(job,Paths);
	
	FileOutputFormat.setOutputPath(job, new Path(routput+thenumber));
	
	//FileOutputFormat.setCompressOutput(job, true);
    //FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
	//SequenceFileOutputFormat.setOutputCompressionType(job,CompressionType.BLOCK);
	//output first level Q
	MultipleOutputs.addNamedOutput(job,
                                     QF_MAT,
                                     SequenceFileOutputFormat.class,
                                     IntWritable.class,
                                     LMatrixWritable.class);
	
	RunningJob rj = JobClient.runJob(job);
	System.out.println("QJob Job ID: "+rj.getJobID().toString());
	rinput = routput+thenumber;
	}
  }

}
