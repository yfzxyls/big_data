#!/bin/bash
if find -name RUNNING_PID | grep "RUNNING_PID";
then
  echo "PROCESS is running..."
  exit
fi

nohup /opt/module/kafka-manager-1.3.3.15/bin/kafka-manager  -Dconfig.file=/opt/module/kafka-manager-1.3.3.15/conf/application.conf -Dhttp.port=8080 & > nohup.out

if [ ! -z "RUNNING_PID" ]; then
  echo $! > RUNNING_PID
fi 
