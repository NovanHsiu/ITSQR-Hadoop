"creatRMat.java" and "QReduceTest.java" are codes for experiment of performance comparison between varying setup of argument¡§reduceSchedule".

Usage Example:
(1) First, create synthesized matrices R (That is square matrix):
${HADOOP_HOME}/bin/hadoop jar ${TSQR_DIR}/TSQR.jar nthu.scopelab.tsqr.test.createRMat -libjars '$(MTJ)' -output outputSR -size 100 -number 100
(2) Last, run the reduceSchedule testing program for those R matrices
${HADOOP_HOME}/bin/hadoop jar ${TSQR_DIR}/TSQR.jar nthu.scopelab.tsqr.test.QReduceTest -libjars '$(MTJ)' -input outputSR -output outputTest -colsize 100 -reduceSchedule 4,1 -mis 64

For more details, please read the section 5.4.2 in my thesis "Implemenations of TSQR for Cloud Platforms and Its Applications of SSVD and Collaborative Filtering"