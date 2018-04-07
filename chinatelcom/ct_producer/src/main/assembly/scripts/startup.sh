#!/bin/bash

MAINJAR=ct_producer-1.0-SNAPSHOT.jar

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

$JAVA_HOME/bin/java -Dfile.encoding=UTF-8 -Dproducer=prod  com.soap.chaintelcom.producer.Producer  &> $PROCESS_HOME/out.log &

if [ ! -z "RUNNING_PID" ]; then
  echo $! > RUNNING_PID
fi