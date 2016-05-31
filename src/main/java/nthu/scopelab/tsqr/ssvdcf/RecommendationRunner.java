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
// 2014 Hsiu-Cheng Yu
package nthu.scopelab.tsqr.ssvdcf;

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

import nthu.scopelab.tsqr.ssvd.SSVDRunner;

/**
 * RecommendationRunner
 * ---
 *
 * 
 * Example Usage:
 *
 */
public class RecommendationRunner extends TSQRunner {

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
        rs_str = "1";
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
	String nredtask_str = getArgument("-reduceTasks",args);
	if (nredtask_str == null) {
		nredtask_str = "1";
	}
	String th_str = getArgument("-thItem",args);
	if(th_str==null) {
		th_str = "1";
	}
	String nrec_str = getArgument("-numRec",args);
	if(nrec_str==null) {
		nrec_str = "10";
	}
	String job3_str = getArgument("-job3",args);
	if(job3_str==null) {
		job3_str = "true";
	}
	
    int k = Integer.parseInt(k_str);
    int p = Integer.parseInt(p_str);
    int h = Integer.parseInt(h_str);
    int abh = Integer.parseInt(abh_str);
    int q = Integer.parseInt(q_str);
    boolean computeU = Boolean.parseBoolean(cu_str);
    boolean computeV = Boolean.parseBoolean(cv_str);
    boolean cUHalfSigma = Boolean.parseBoolean(uhs_str);
    boolean cVHalfSigma = Boolean.parseBoolean(vhs_str);
    int reduceTasks = Integer.parseInt(t_str);
	String reduceSchedule = rs_str;
	int subRowSize = Integer.parseInt(srs_str);
	int mis = Integer.parseInt(mis_str);
    boolean broadcast = Boolean.parseBoolean(br_str);
	boolean job3 = Boolean.parseBoolean(job3_str);
	int itemThreshold = Integer.parseInt(th_str);
    boolean overwrite = true;
	int numRecommendations = Integer.parseInt(nrec_str);
	long stime, etime, t1, t2;	
	
    Configuration conf = getConf();
    if (conf == null) {
      throw new IOException("No Hadoop configuration present");
    }
	FileSystem fs = FileSystem.get(conf);
	String seqMatrixPath = outputpath+"/seqMatrix";
	
	int info;

	stime = new Date().getTime();
	//job1 - preparation
	t1 = stime;
    info = ToolRunner.run(conf, new PreparationJob(), new String[]{
        "-input", inputpath,
        "-output", seqMatrixPath,
        "-subRowSize", srs_str,
		"-reduceTasks",t_str,
	    "-mis",mis_str});
	if(info==-1)
		return -1;
	t2 = new Date().getTime();
	System.out.println("Preparation-Job Finished in: "+(t2-t1));
	//job2 - ssvd	
	t1 = t2;
	info = ToolRunner.run(conf, new SSVDRunner(), new String[]{
        "-input",seqMatrixPath,
		"-output",outputpath,
		"-rank",k_str,
		"-oversampling",p_str,
		"-outerProdBlockHeight",h_str,
		"-abtBlockHeight",abh_str,
		"-computeU",cu_str,
		"-computeV",cv_str,
		"-uHalfSigma",uhs_str,
		"-vHalfSigma",vhs_str,
		"-reduceTasks",t_str,
		"-powerIter",q_str,
		"-broadcast",br_str,
		"-subRowSize",srs_str,
		"-reduceSchedule",rs_str,
		"-hasSeqMat","true",
		"-mis",mis_str});
	if(info==-1)
		return -1;
	
	t2 = new Date().getTime();
	System.out.println("SSVD-Job Finished in: "+(t2-t1));
	//job3 - recommendation
	t1 = t2;
	String ssvdPath = outputpath+"/ssvd";
	String recPath = outputpath+"/recommendation";
	RecommendationJob recJob = new RecommendationJob();
	recJob.start(conf,ssvdPath+"/U", recPath, ssvdPath+"/Sigma/svalues.seq", ssvdPath+"/V", itemThreshold,numRecommendations,mis);
	
	t2 = new Date().getTime();
	System.out.println("Recommendation-Job Finished in: "+(t2-t1));
	etime = t2;

	System.out.println("All MapReduce Job Finished in: "+(etime-stime));
	return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RecommendationRunner(), args);
  }

}
