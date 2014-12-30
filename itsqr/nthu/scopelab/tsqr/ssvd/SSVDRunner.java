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
//modify from mahout-6.0 package org.apache.mahout.math.hadoop.stochasticsvd.SSVDCli
// 2013 Hsiu-Cheng Yu
package nthu.scopelab.tsqr.ssvd;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ToolRunner;

import no.uib.cipr.matrix.DenseVector;
import nthu.scopelab.tsqr.matrix.VectorWritable;
import nthu.scopelab.tsqr.TSQRunner;
import nthu.scopelab.tsqr.SequenceFileMatrixMaker;

/**
 * SSVDRunner
 * ---
 * part of Modification:
 * 1. Add a preparative job to produce SequenceFile Matrix
 * 2. Remove part of housekeeping.
 * 
 * Example Usage:
 
	${HADOOP_HOME}/bin/hadoop jar ${TSQR_PACKAGE}/TSQR.jar nthu.scopelab.tsqr.ssvd.SSVDRunner \
	-input mat/100kx100 -output tsqrOutput -subRowSize 4000 -rank 10 -reduceTasks 8 \
	-reduceSchedule 4,1 -mis 64
 */
public class SSVDRunner extends TSQRunner {

  @Override
  public int run(String[] args) throws Exception {
  
	String inputpath = getArgument("-input",args);
    if (inputpath == null) {
            System.out.println("Required argument '-input' missing");
            return -1;
    }                
    String outputpath = getArgument("-output",args);
    if (outputpath == null) {
            System.out.println("Required argument '-output' missing");
            return -1;
    }
	//decomposition rank
	String k_str = getArgument("-rank",args);
	if (k_str == null) {
        System.out.println("Required argument '-rank' missing");
        return -1;
    }
	//oversampling
	String p_str = getArgument("-oversampling",args);
	if (p_str == null) {
        p_str = "15";
    }
	//Y block height (must be > (k+p))
	String r_str = getArgument("-blockHeight",args);
	if (r_str == null) {
        r_str = "10000";
    }
	//block height of outer products during multiplication, increase for sparse inputs
	String h_str = getArgument("-outerProdBlockHeight",args);
	if (h_str == null) {
        h_str = "30000";
    }
	//block height of Y_i in ABtJob during AB' multiplication, increase for extremely sparse inputs
	String abh_str = getArgument("-abtBlockHeight",args);
	if (abh_str == null) {
        abh_str = "200000";
    }
	String mss_str = getArgument("-minSplitSize",args);
	if (mss_str == null) {
        mss_str = "-1";
    }
	String cu_str = getArgument("-computeU",args);
	if (cu_str == null) {
        cu_str = "true";
    }
	//Compute U as UHat=U x pow(Sigma,0.5)
	String uhs_str = getArgument("-uHalfSigma",args);
	if (uhs_str == null) {
        uhs_str = "false";
    }
	String cv_str = getArgument("-computeV",args);
	if (cv_str == null) {
        cv_str = "true";
    }
	//compute V as VHat= V x pow(Sigma,0.5)
	String vhs_str = getArgument("-vHalfSigma",args);
	if (vhs_str == null) {
        vhs_str = "false";
    }
	String t_str = getArgument("-reduceTasks",args);
	if (t_str == null) {
        System.out.println("Required argument '-reduceTasks' missing");
        return -1;
    }
	//number of additional power iterations (0..2 is good)
	String q_str = getArgument("-powerIter",args);
	if (q_str == null) {
        q_str = "0";
    }
	//whether use distributed cache to broadcast matrices wherever possible
	String br_str = getArgument("-broadcast",args);
	if (br_str == null) {
        br_str = "true";
    }
	String rs_str = getArgument("-reduceSchedule",args);
	if (rs_str == null) {
        System.out.println("Required argument '-reduceSchedule' missing");
        return -1;
    }
	String srs_str = getArgument("-subRowSize",args);
	if (srs_str == null) {
        System.out.println("Required argument '-subRowSize' missing");
        return -1;
    }
	String mis_str = getArgument("-mis",args);
	if (mis_str == null) {
        mis_str = "64";
    }
	String j1t_str = getArgument("-job1ReduceTasks",args);
	if (j1t_str == null) {
		j1t_str = "1";
	}
	
	//hasSeqMat == ture means that the input file is JTSQR SequnceFile, otherwise is TextFile
	String seqm_str = getArgument("-hasSeqMat",args);
	if(seqm_str==null) {
		seqm_str = "false";
	}

    int k = Integer.parseInt(k_str);
    int p = Integer.parseInt(p_str);
    int r = Integer.parseInt(r_str);
    int h = Integer.parseInt(h_str);
    int abh = Integer.parseInt(abh_str);
    int q = Integer.parseInt(q_str);
    int minSplitSize = Integer.parseInt(mss_str);
    boolean computeU = Boolean.parseBoolean(cu_str);
    boolean computeV = Boolean.parseBoolean(cv_str);
    boolean cUHalfSigma = Boolean.parseBoolean(uhs_str);
    boolean cVHalfSigma = Boolean.parseBoolean(vhs_str);
    int reduceTasks = Integer.parseInt(t_str);
	String reduceSchedule = rs_str;
	int subRowSize = Integer.parseInt(srs_str);
	int MaxInputSplitSize = Integer.parseInt(mis_str);
    boolean broadcast = Boolean.parseBoolean(br_str);
    boolean overwrite = true;
    boolean hasSeqMat = Boolean.parseBoolean(seqm_str);

    Configuration conf = getConf();
    if (conf == null) {
      throw new IOException("No Hadoop configuration present");
    }
	FileSystem fs = FileSystem.get(conf);
	String subMatrixCount_output = outputpath+"/subMatrixCount/partitionList";
	String seqMatrix_output = outputpath+"/seqMatrix";
	
     if(!hasSeqMat)
     {
	//job 1
	//Job 1-1: Create sub matrix number list
	//Job 1-2: Generate sequential sub matrix depend on number_list
	SequenceFileMatrixMaker mat_maker = new SequenceFileMatrixMaker();
	int info = ToolRunner.run(getConf(), mat_maker, new String[]{
              "-input", inputpath,
              "-subMatrixCount_output", subMatrixCount_output,
			  "-seqMatrix_output", seqMatrix_output,
              "-subRowSize", srs_str,
			  "-mis",mis_str});	  
	if(info==-1)
	 return -1;
     }
     else
     {
	seqMatrix_output = inputpath; 
     }	
	//ssvd
	Path ssvdOutputPath = new Path(outputpath+"/ssvd");
    SSVDSolver solver =
      new SSVDSolver(conf,
                     new Path[] { new Path(seqMatrix_output) },
                     ssvdOutputPath, //modify
                     r,
                     k,
                     p,
                     reduceTasks,
					 subRowSize,
					 reduceSchedule,
					 MaxInputSplitSize);
    solver.setMinSplitSize(minSplitSize);
    solver.setComputeU(computeU);
    solver.setComputeV(computeV);
    solver.setcUHalfSigma(cUHalfSigma);
    solver.setcVHalfSigma(cVHalfSigma);
    solver.setOuterBlockHeight(h);
    solver.setAbtBlockHeight(abh);
    solver.setQ(q);
    solver.setBroadcast(broadcast);
    solver.setOverwrite(overwrite);

    solver.run();
    // remove housekeeping
    return 0;
  }

  public static void main(String[] args) throws Exception {
    long start, end;
    start = new Date().getTime();
    ToolRunner.run(new SSVDRunner(), args);
    end = new Date().getTime();
    System.out.println("Finished in: "+(end-start));
  }

}
