#!/bin/bash

WORK_DIR=`pwd`

HADOOP_HOME=/data1/hadoop-0.21.0

#CLASSPATH=cloudetl.jar:${HADOOP_HOME}/hadoop-common-0.21.0.jar:${HADOOP_HOME}/hadoop-mapred-tools-0.21.0.jar:${HADOOP_HOME}/hadoop-common-0.21.0.jar:${HADOOP_HOME}/hadoop-hdfs-0.21.0.jar:${HADOOP_HOME}/hadoop-mapred-0.21.0.jar

#cd ${HADOOP_HOME}
#for i in $( ls lib/*.jar )
#do
#   CLASSPATH=${CLASSPATH}:${HADOOP_HOME}/${i} 
#done

#cd ${WORK_DIR}
#java  -cp ${CLASSPATH} dk.aau.cs.cloudetl.examples.CloudETLTest $1

cd ${HADOOP_HOME}/bin
. hadoop jar /home/xiliu/workspace/cloudetl/build/dist/cloudetl.jar xiliu-fedora $1 

cd ${WORK_DIR}
