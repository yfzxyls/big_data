#!/bin/bash

MAINJAR=ct_count_duration-1.0-SNAPSHOT.jar

if find -name process.pid | grep "process.pid";
then
  echo "PROCESS is running..."
  exit
fi

if [ -z $PROCESS_HOME ]; then
        PROCESS_HOME=..;
fi

CLASSPATH=$CLASSPATH
for i in $PROCESS_HOME/lib/*
do
     CLASSPATH=$i:$CLASSPATH
done
export CLASSPATH

/opt/module/hadoop-2.7.2/bin/yarn jar  ../lib/ct_count_duration-1.0-SNAPSHOT.jar com.soap.ct.mr.CountDurationRunner  &> $PROCESS_HOME/out.log &

if [ ! -z "process.pid" ]; then
  echo $! > process.pid
fi