ITSQR: Iterative Tall-and-Skinny QR Factorization in Hadoop MapReduce Implementation
======
* Version 0.1
* Distributed in 2014/09/04

## Author
Hsiu-Cheng Yu, Che-Rung Lee


## Introduction
The function provided by this package is a QR factorizations for Tall-and-Skinny matrices (TSQR) in Hadoop MapReduce implementation.  

A 'Tall-and-Skinny matrix' which number of rows is much bigger than its number of columns. The TSQR function can be used to solve many problems, such as stochastic SVD (SSVD). You can find more detail about this implemenation in my thesis *Implemenations of TSQR for Cloud Platforms and Its Applications of SSVD and Collaborative Filtering*.

In this implementation, we provide three functions:

 * TSQR: Compute Q and R matrices of a given tall-and-skinny matrix.
 * Q^T\*B: Apply the Q factor to a matrix B.
 * SSVD: Compute the stochastic SVD. This is an application of TSQR. 
 * Recommendation System: An application of SSVD, Use item-based collaborative filtering to implement a recommendation system
 
This code is written in Java and uses JLAPACK and JBLAS library for matrix computation.

## Prerequisite
 * JDK 7
 * Apache Maven
 * Apache Hadoop 1.x version 

## Compile and Package 
 Our project is manager by maven.

 - `$ mvn compile`
    - Compile
 - `$ mvn install`
    - Compile and then package source code 

## Usage Example and Command Line Arguments

Using maven for comiple and package that would download dependencies (library) at `{HOME}/.m2/repository`. Make sure every machines on hadoop cluster can import following jar files. I suggest compiling once and copy libraries to other machines.

* ${MTJ}
  - Path of matrix-toolkits-java package
  - (default path: `${HOME}/.m2/repository/com/googlecode/matrix-toolkits-java/mtj/0.9.14/mtj-0.9.14.jar`)
* ${F2J_ARPACK}
  - Path of f2j arpack package
  - (default path: `${HOME}/.m2/repository/net/sourceforge/f2j/arpack_combined_all/0.1/arpack_combined_all-0.1.jar`)
* ${HADOOP_HOME}
  - Path where Hadoop is installed
* ${TSQR_DIR}
  - Path where TSQR package is installed
  
### Upload our test data for experiment of ITSQR or SSVD
```
 $ hadoop fs -mkdir tsqr
 $ hadoop fs -copyFromLocal testdata/100x5
```
In the input file of example, one line means one row in a matrix, and numbers are separated by a space. 
If you want to use other matrices for this code, the format of matrices must follow above rules.

### Upload our test data for experiment of Recommendation System
```
$ hadoop fs -mkdir ratingData`
$ hadoop fs -copyFromLocal testdata/testRatingData`
```
 
 The input of recommendation system is composed by user ratings for items in text file, looks like following example:
```
 <user id> <item id> <rating value>
 108 123 3.5
```

### Example: Run Iterative TSQR
* Do QR Factorization only
```
${HADOOP_HOME}/bin/hadoop jar ${TSQR_DIR}/target/TSQR-0.1.jar nthu.scopelab.tsqr.TSQRunner \
 -libjars '${MTJ},${F2J_ARPACK}' \
 -input mat/100x5 \
 -output tsqrOutput \
 -subRowSize 10 \
 -reduceSchedule 4,1 \
 -mis 64 \
 -type 0
```

### Compute Q^T\*A
```
${HADOOP_HOME}/bin/hadoop jar ${TSQR_DIR}/target/TSQR-0.1.jar nthu.scopelab.tsqr.TSQRunner \
 -libjars '${MTJ},${F2J_ARPACK}' \
 -input mat/100x5 \
 -output tsqrOutput \
 -inputB mat/100x5 \
 -outputQ true \
 -subRowSize 10 \
 -reduceSchedule 4,1 \
 -mis 64 \
 -type 1
```

### Explanation of Arguments for Running Iterative TSQR
The argument has star mark * means that it must be given a value by user and other arguments without star mark have default value.

* `-libjar` *
  - Path of imported libraries.
* `-input` *
  - Input file, including its directory.
* `-inputB` *
  - The *inputB* argument is necessary if the *type* argument set to 1.
  - The matrix used to compute Q^T\*B. Notice that the number of rows of B matrix must equal to that of A matrix(the original matrix).
* `-output` *
  - Output directory
* `-outputQ` (default: false)
  - This argument only used in compute Q^T\*B. If it is true, QMultiplyJob outputs the Q matrix to a file.  False outputs R matrix only.
* `-subRowSize` (default: equal to number of columns)
  - It is the number of rows of each submatrix. These submatrices are split from input matrix. subRowSize must be bigger than the number of columns, and smaller than the number of rows of the original matrix.
* `-reduceSchedule` (default: 1) 
  - This argument is used in QRFirstJob. The input can be a sequence of numbers, which are separated by commas.  The ith number in the sequence represents the number of Reducers in the ith MapReduce task. Finally, last number need to set to one in order to calculate the final R.
  - For example, 4,1 means that it has two MapReduce and first MapReduce has four Reducers and second has one reducer.
* `-mis` (default: 64)
  - Max Input Split size: the maximum size of each input data.  For example, mis=64 means the maximum size of each split file is 64MB. 
* `-type` *
  - 0 means that do QR factorization only and 1 is to compute Q^T\*B

### Example: Run SSVD
```
${HADOOP_HOME}/bin/hadoop jar ${TSQR_DIR}/target/TSQR-0.1.jar nthu.scopelab.tsqr.ssvd.SSVDRunner \
 -libjars '${MTJ},${F2J_ARPACK}' \
 -input mat/100kx500 \
 -output tsqrOutput \
 -subRowSize 4000 \
 -reduceSchedule 4,1 \
 -mis 64 \
 -rank 10 \
 -oversampling 50 \
 -reduceTasks 8
```

### Explanation of Arguments for Running SSVD
 * input, output, ....
   - The same as they are in the TSQR example
 * rank
   - *rank* is Stochastic number that would simply m\*n matrix to m\*k matrix. (k is smaller than n)
 * oversampling
   - *oversampling* is a sampling value for reducing deviation of SSVD and this value would increase the size of column in calculation
 * reduceTasks *
   - Number of Reduce tasks of BtJob, UJob and VJob

### Example: Running Iterative TSQR SSVD (ITSSVD) Item-Based Recommendation System 
```
${HADOOP_HOME}/bin/hadoop jar ${TSQR_DIR}/target/TSQR-0.1.jar nthu.scopelab.tsqr.ssvdcf.RecommendationRunner \
 -libjars '${MTJ}' \
 -input ratingData/testRatingData \
 -output outputRS \
 -reduceTasks 16 \
 -rank 10 \
 -oversampling 50 \
 -subRowSize 1000 \
 -numRec 10
```

### Explanation of Arguments for Running Recommendation System 
 * input, output, ....
  The same as they are in the SSVD example
 * numRec
  Number of recommended item for per user
  
### Example: Turn the matrix from Hadoop sequencefile into text file and then print out 
```
${HADOOP_HOME}/bin/hadoop jar ${TSQR_DIR}/target/TSQR-0.1.jar nthu.scopelab.tsqr.rs.readSFMat \
-libjars '${MTJ},${F2J_ARPACK}' \
 input tsqrOutput/part-00000
```

## Tuning Suggestion of Arguments
If you want improve performance for ITSQR by tuning argument. We have some tips for following three arguments.

* `-subRowSize`

	This argument does not has obvious influence for performance of application in this package, but it has a point need to know. If the argument is too large which cause the data size of sub matrix is bigger than Map task input split size, that will dramatically reduce the performance because of unblance workload for each task. In order to avoid above situation, the program would automatically reduce the subRowSize in ITSQR if that value is too large.

* `-mis`

	In most cases, reduce the mis would lightly improve the performance. That means that you increase the number of Map tasks. But you launch too many Map tasks (too many JVM processes) that would cause memory requirement exceed the free memory of machine and the Hadoop file system corrupted. In order to avoid above situation, it would automatically check the value of mis in ITSQR.

* `-reduceSchedule`

	The default value of this argument was one (1). If you encounter the out of memory problem of JVM or performance bottleneck in QRFirstJob or QJob that caused by bigger data, you could find more detail in section 5.4.2 of my thesis which is descirbed in above part of introduction.

## Overview
 * `itsqr/nthu/scopelab/tsqr/TSQRunner.java` - driver code for TSQR
 * `itsqr/nthu/scopelab/tsqr/SequencefileMatrixMaker.java` - driver code for MapReduce code, which turns the matrix from text file to Hadoop sequencefile 
 * `itsqr/nthu/scopelab/tsqr/ssvd/SSVDRunner.java` - driver code for SSVD
 * `itsqr/nthu/scopelab/tsqr/ssvdcf/RecommendationRunner.java` - driver code for Recommendation System

## References
 * Direct QR factorizations for tall-and-skinny matrices in MapReduce architectures [pdf](http://arxiv.org/abs/1301.1071)
 * Tall and skinny QR factorizations in MapReduce architectures [pdf](http://www.cs.purdue.edu/homes/dgleich/publications/Constantine%202011%20-%20TSQR.pdf)
 * [MAHOUT-376] Implement Map-reduce version of stochastic SVD [website](https://issues.apache.org/jira/browse/MAHOUT-376)

## Contact
 * For any questions, suggestions, and bug reports, email Hsiu-Cheng Yu by s411051@gmail.com please.
 * This code can be reached at: https://github.com/NovanHsiu/ITSQR-Hadoop
