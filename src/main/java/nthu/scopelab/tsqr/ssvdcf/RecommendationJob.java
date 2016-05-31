/**
 * Recommend Top-k item to user
--------------
 Input: 
	U, Sigma and V factor
 Output: 
	Recommendation vector which has Top-k item with score of preference for per user 
**/
package nthu.scopelab.tsqr.ssvdcf;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FileStatus;
import java.io.IOException;
import java.lang.Exception;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.lang.Long;
import java.lang.Boolean;
import java.util.Arrays;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Comparator;
import java.util.Iterator;

import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.VectorEntry;
import no.uib.cipr.matrix.Matrix;
import no.uib.cipr.matrix.TridiagMatrix;
import no.uib.cipr.matrix.sparse.SparseVector;

import nthu.scopelab.tsqr.math.QRFactorMultiply;
import nthu.scopelab.tsqr.matrix.VectorWritable;
import nthu.scopelab.tsqr.matrix.LMatrixWritable;
import nthu.scopelab.tsqr.matrix.cmDenseMatrix;

import org.netlib.blas.Dscal;

public class RecommendationJob{
  public enum RJobTime { Computation, Total }
  private static final boolean DEBUG = false;
  
  private static final Logger log = LoggerFactory.getLogger(RecommendationJob.class);
  
  static final String VMATRIX_PATH = "vmatrixPath";
  static final String SIGMA_PATH = "sigmaPath";
  static final String THRESHOLD = "threshold";
  static final String NUM_RECOMMENDATIONS = "numRecommendations";  
  static final String ITEMS_FILE = "itemsFile";
  
 public static void start(Configuration conf,String UPath, String outputPath, String SigmaPath, String vPath, int thItem, int numRecommendations, int mis) 
 throws Exception {
	FileSystem.get(conf).delete(new Path(outputPath),true);

    Job job = new Job(conf);
    job.setJobName("RecommendationJob");
    job.setJarByClass(RecommendationJob.class);
	
	job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
	
	FileInputFormat.setInputPaths(job, new Path(UPath));
	FileInputFormat.setMaxInputSplitSize(job,mis*1024*1024);
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	
	job.setMapperClass(TimesSquaredMapper.class);		
	//job.setReducerClass(IdentityReducer.class); 
	job.setNumReduceTasks(0);
	
	job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(RecommendedItemsWritable.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(RecommendedItemsWritable.class);
	
	job.getConfiguration().set(SIGMA_PATH, SigmaPath);
	job.getConfiguration().set(VMATRIX_PATH, vPath);
	job.getConfiguration().setInt(THRESHOLD, thItem );
	job.getConfiguration().setInt(NUM_RECOMMENDATIONS, numRecommendations);
	job.waitForCompletion(true);
    }
	
/**
 * maps a row of the similarity matrix to a {@link VectorOrPrefWritable}
 *
 * actually a column from that matrix has to be used but as the similarity matrix is symmetric, 
 * we can use a row instead of having to transpose it
 */
public static class TimesSquaredMapper extends
    Mapper<LongWritable,LMatrixWritable,LongWritable,RecommendedItemsWritable> {

  private Path sigmaPath;
  private Configuration conf;
  private Vector sigma;
  private Path VmatrixPath;
  private int threshold;
  private static final int DEFAULT_NUM_RECOMMENDATIONS = 10;
  private static final int DEFAULT_THRESHOLD = 1000;
  private int recommendationsPerUser;
  private PathFilter trainFilter;
  private FileSystem fs;
  private long s0, e0, s1, e1, sTopkTime=0, whileVTime=0, UsVTime=0;
  private int taskId; 
  private cmDenseMatrix inputMat = null, outputMat = null; 
  private Path[] localFiles;
  
  private LongWritable vkey = new LongWritable();
  private LMatrixWritable vvalue = new LMatrixWritable();
  private LongWritable okey = new LongWritable();
  private RecommendedItemsWritable ovalue = new RecommendedItemsWritable();
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    conf = context.getConfiguration();
	taskId = Integer.parseInt(conf.get("mapred.task.id").split("_")[4]);
	fs = FileSystem.get(conf);

    VmatrixPath = new Path(conf.get(VMATRIX_PATH));
    VmatrixPath = VmatrixPath.makeQualified(FileSystem.get(conf));

    sigmaPath = new Path(conf.get(SIGMA_PATH));
    sigmaPath = sigmaPath.makeQualified(fs);

    threshold = conf.getInt(THRESHOLD, DEFAULT_THRESHOLD);
//--------------------------------------------------------------------------

    recommendationsPerUser = conf.getInt(NUM_RECOMMENDATIONS, DEFAULT_NUM_RECOMMENDATIONS);

     /* get sigma Vector */
	sigma = null;
    SequenceFile.Reader reader = null;
	
    try{
        reader = new SequenceFile.Reader(fs, sigmaPath, conf);
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        VectorWritable value = (VectorWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        if (reader.next( key,value )){
            sigma = value.get();
        }
    } finally {
        IOUtils.closeStream(reader);
    }
	if(sigma==null)
	 throw new NullPointerException("Sigma size is null!");
	
	//sigma^3
	for(int i=0;i<sigma.size();i++)
	 sigma.set(i,sigma.get(i)*sigma.get(i)*sigma.get(i));//Math.exp((sigma.get(i)*sigma.get(i))/1000000)); // original: sigma^3 -> modify into: sigma*e^(sigma^2)
  }

  protected void map(LongWritable key,
                     LMatrixWritable value,
                     Context context) throws IOException, InterruptedException {
	 //initial arguments
	  
	 FileSystem fs = FileSystem.get(new Configuration());
	 double d = 0.0;
	 int index = 0;
	 int m, n;
	 cmDenseMatrix inputMat = value.getDense();
	 m = inputMat.numRows();
	 n = inputMat.numColumns();//sigmaMatrix.numColumns();

	 //U x(sigma^3) use dscal to doing matrix multiplication
	 for(int i=0;i<n;i++)
	  Dscal.dscal(m,sigma.get(i),inputMat.getData(),i*m,1);
    
	//construct v squencefile reader
	FileStatus[] vfileStatus = fs.listStatus(VmatrixPath,new QRFactorMultiply.MyPathFilter("v-"));	
	SequenceFile.Reader sreader;

	long[] userIndex = value.getLongArray();
	int userIndexLength = value.getLongLength();
	List<TopkItem> topkItemList = new ArrayList<TopkItem>();
	for(int i=0;i<userIndexLength;i++)
	 topkItemList.add(new TopkItem(recommendationsPerUser));
	 
	//results matrix row size
	m = inputMat.numRows(); 
    try{
	for(int vi=0;vi<vfileStatus.length;vi++)
	{
	  sreader = new SequenceFile.Reader(fs,vfileStatus[vi].getPath(),fs.getConf());
	  while(sreader.next(vkey,vvalue))
	  {
	  cmDenseMatrix VMat = vvalue.getDense();
	  
	  // (U x  (sigma^3)) x V' 
	  //results matrix col size
	  n = VMat.numRows();
	  if(outputMat==null)
	  {
	   outputMat = new cmDenseMatrix(new double[m*n*2],m,n);
	  }
	  else if(outputMat.numRows()*outputMat.numColumns()<m*n)
	   outputMat = new cmDenseMatrix(new double[m*n*2],m,n);
	  
	  outputMat = QRFactorMultiply.Multiply("N","T",inputMat,VMat,outputMat);
	  long[] itemIndex = vvalue.getLongArray();
	  
	  //select top k item from recommendationVector
	  for(int i=0;i<m;i++)
	  {
	   Vector recommendationVector = new SparseVector(Integer.MAX_VALUE);
	   for(int j=0;j<n;j++)
	   {
		d = outputMat.getData()[j*m+i];
		index = (int) itemIndex[j];
		//recommendationVector.set( index , d ); //testing
	    if( d > threshold )
		{
	        recommendationVector.set( index , (double) (d/threshold) );
			//System.out.println("test threshold!");
		}
	   }//for 2 item
		
		for (VectorEntry ve : recommendationVector) {
		 index = ve.index();
		 long itemID;
		 //we don't have any mappings, so just use the original
		 itemID = index;
		 double dvalue = ve.get();
		 if (!Double.isNaN(dvalue)) {
		  //System.out.println("test offer!");
          topkItemList.get(i).offer(itemID, dvalue);
         }
		}
	  }//for 1 user
	  }//while: sreader
	  sreader.close();
	}//for: vfilestatus
	 for(int i=0;i<m;i++)
	 {
	   //System.out.println("numItem: "+topkItemList.get(i).numItem());
	   if (topkItemList.get(i).numItem()>0)
	   {
		okey.set(userIndex[i]);
		ovalue.set(topkItemList.get(i));
		context.write(okey,ovalue);
	   }
	 }
    }//try
	catch (Exception e){
	 e.printStackTrace();
    }
  }
  
  @Override
  protected void cleanup(Context context) throws IOException
  {
  }

}
}
