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
// modify from mahout-6.0 package org.apache.mahout.math.hadoop.stochasticsvd.ABtDenseOutJob
// 2013 Hsiu-Cheng Yu
package nthu.scopelab.tsqr.ssvd;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.lang.Math;
import java.security.InvalidAlgorithmParameterException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.SequenceFile;

import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.DenseMatrix;
import no.uib.cipr.matrix.MatrixEntry;
import no.uib.cipr.matrix.sparse.FlexCompRowMatrix;
import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.DenseVector;

import nthu.scopelab.tsqr.matrix.LMatrixWritable;
import nthu.scopelab.tsqr.matrix.SparseRowBlockWritable;
import nthu.scopelab.tsqr.matrix.SparseRowBlockAccumulator;
import nthu.scopelab.tsqr.matrix.VectorWritable;
import nthu.scopelab.tsqr.matrix.cmDenseMatrix;
import nthu.scopelab.tsqr.matrix.cmUpperTriangDenseMatrix;
import nthu.scopelab.tsqr.TSQRunner;
import nthu.scopelab.tsqr.math.QRFactorMultiply;
import nthu.scopelab.tsqr.QmultiplyJob;
import nthu.scopelab.tsqr.TSQRunner.fileGather;
import nthu.scopelab.tsqr.TSQRunner.Checker;

/**
 * Computes ABt products, then first step of QR which is pushed down to the
 * reducer.
 *---
 * part of Modification:
 * 1. Remove QRReducer. (We could use QMapper of QJob to do the same job as QRReducer)
 */
 
@SuppressWarnings("unchecked")
public class ABtDenseOutJob{
  private static final boolean debug = false;
  public static final String PROP_BT_PATH = "ssvd.Bt.path";
    
  public static void run(Configuration conf,
						 Path[] inputPath,
						 Path inputBt,
						 Path outputPath,
						 int k,
						 int p,
						 int reduceTasks,
						 int mis) 
						 throws Exception {
						 		
	JobConf job = new JobConf(conf, ABtDenseOutJob.class);
	job.setInputFormat(SequenceFileInputFormat.class);
	job.setOutputFormat(SequenceFileOutputFormat.class);
	
    job.setInt(QJob.PROP_K, k);
    job.setInt(QJob.PROP_P, p);
    job.set(PROP_BT_PATH, inputBt.toString());

	FileOutputFormat.setOutputPath(job, outputPath);
	job.setJobName("ABtDenseOutJob");
	
	job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(LMatrixWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(LMatrixWritable.class);
	
	job.setMapperClass(ABtMapper.class);		
	
	fileGather fgather = new fileGather(inputPath,"",FileSystem.get(job));
	mis = Checker.checkMis(mis,fgather.getInputSize(),FileSystem.get(job));
	job.setNumMapTasks(fgather.recNumMapTasks(mis));
	
    job.setNumReduceTasks(0);
	
	FileInputFormat.setInputPaths(job,inputPath);
	
	 RunningJob rj = JobClient.runJob(job);	
    }

	public static class ABtMapper
        extends QmultiplyJob.readAtoBuildQMethod implements Mapper<IntWritable, LMatrixWritable, IntWritable, LMatrixWritable>{	
		
		private cmDenseMatrix yiRows;
		private Vector[] Bt = null;
		private int aRowCount;
		private int kp;
		private int blockHeight;
		private boolean distributedBt;
		private Configuration localFsConfig;
		private Path BtPath;
		private FileSystem fs;
		private LMatrixWritable ovalue = new LMatrixWritable();
		private List<Vector> Btlist = new ArrayList<Vector>();
		private List<Integer> BtIndexlist = new ArrayList<Integer>();
		
		@Override
		public void configure(JobConf job){
		  int k = Integer.parseInt(job.get(QJob.PROP_K));
		  int p = Integer.parseInt(job.get(QJob.PROP_P));
		  kp = k + p;
		  BtPath = new Path(job.get(PROP_BT_PATH));
		  try
		  {
		  fs = FileSystem.get(job);
		  }
		  catch(Exception e)
		  {
		   e.printStackTrace();
		  }
		}
		
        public void map(IntWritable key, LMatrixWritable value, OutputCollector<IntWritable, LMatrixWritable> output, Reporter reporter)
            throws IOException {
			
			yiRows = new cmDenseMatrix(value.matNumRows(),kp);
			
			//load B
			if(Btlist.size()<=0)
			{
			 //preload all Bt matrix in memory
			 preloadBt(value);
			}
			
			for(int i=0;i<value.matNumRows();i++)
			{
             Vector btVec;
			 if(value.isDense())
			 {			  
              for (int j=0;j<value.matNumColumns();j++) {
				btVec = Btlist.get(j);
                double avalue = value.getDense().get(i,j);
                for (int s = 0; s < kp; s++) {
                  yiRows.set(i,s,yiRows.get(i,s)+avalue * btVec.get(s));
                }
              }		  			  
			 }//if value.isDense
			 else
			 {
			    int btIndex;
                for (Iterator<VectorEntry> iter = value.getSparse().getRow(i).iterator(); iter.hasNext();) {
				 VectorEntry ve = iter.next();
				 
				btIndex = BtIndexlist.indexOf(ve.index());
			    if(btIndex>=0)
				{
				 btVec = Btlist.get(btIndex);
                 for (int s = 0; s < kp; s++) {
                  yiRows.set(i,s,yiRows.get(i,s)+ve.get() * btVec.get(s));
                 }
				}
               }	 
			 }//else

			}//i<yiRows.numRows()
			
			ovalue.setLMat(value.getLongArray(),yiRows);
			output.collect(key,ovalue);			
        }//map
		
		public void preloadBt(LMatrixWritable value) throws IOException 
		{
		 SequenceFile.Reader sreader = null;
		 IntWritable ikey = new IntWritable();
		 VectorWritable ivalue = new VectorWritable();
		 FileStatus[] fstat = fs.listStatus(BtPath,new QRFactorMultiply.MyPathFilter("part-"));
		 if(value.isDense())
		 {
		  System.out.println("BtPath "+BtPath.getName());
		  System.out.println("fstat.length: "+fstat.length);
		  for(int ifstat=0;ifstat<fstat.length;ifstat++)
		  {
			sreader = new SequenceFile.Reader(fs,fstat[ifstat].getPath(),fs.getConf());
			while(sreader.next(ikey,ivalue))
			{
			 Btlist.add(ivalue.get());
			}
		  }
		 }//if
		 else
		 {
		  for(int ifstat=0;ifstat<fstat.length;ifstat++)
		  {
			sreader = new SequenceFile.Reader(fs,fstat[ifstat].getPath(),fs.getConf());
			while(sreader.next(ikey,ivalue))
			{
			 Btlist.add(ivalue.get());
			 BtIndexlist.add(ikey.get());
			}
		  }
		 }//else
		}
    }
}