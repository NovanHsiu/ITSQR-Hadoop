HADOOP_HOME := /home/hadoop/hadoop
MTJ := /home/hadoop/javalib/mtj-0.9.14.jar
JLAPACK := /home/hadoop/javalib/jlapack-0.8/lapack.jar
JBLAS := /home/hadoop/javalib/jlapack-0.8/blas.jar
JUTIL := /home/hadoop/javalib/jlapack-0.8/f2jutil.jar
COMPILE := $(shell ./compile $(HADOOP_HOME) $(MTJ) $(JLAPACK) $(JBLAS) $(JUTIL))
all: TSQR
	$(COMPILE)
	jar -cvf TSQR.jar -C TSQR/ .
TSQR:
	mkdir TSQR
clean:
	rm -r TSQR
	rm TSQR.jar
