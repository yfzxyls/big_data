#!/bin/bash
export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:/home/hadoop/calllog/jobs/lib/*
/opt/module/hadoop-2.7.2/bin/yarn jar /home/hadoop/calllog/jobs/lib/ct_count_duration-1.0-SNAPSH
OT.jar com.soap.ct.mr.CountDurationRunner