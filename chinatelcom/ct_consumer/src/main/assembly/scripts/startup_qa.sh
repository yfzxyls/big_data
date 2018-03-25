#!/bin/bash

MAINJAR=app_biz_center_provider-1.0.0-SNAPSHOT.jar

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

$JAVA_HOME/bin/java -Ddubbo.service.shutdown.wait=60000 -Ddubbo.shutdown.hook=true -Dfile.encoding=UTF-8 -Dapp_biz_center=prod -Xms1024m -Xmx1024m -Xmn384M -Xss512k -XX:MaxPermSize=256m -XX:PermSize=128m com.alibaba.dubbo.container.Main&>$PROCESS_HOME/out.log &

if [ ! -z "process.pid" ]; then
  echo $! > process.pid
fi