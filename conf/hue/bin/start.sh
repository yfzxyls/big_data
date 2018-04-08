#!/bin/bash
if find -name RUNNING_PID | grep "RUNNING_PID";
then
  echo "PROCESS is running..."
  exit
fi

nohup /opt/module/hue/build/env/bin/supervisor & > nohup.out


if [ ! -z "RUNNING_PID" ]; then
  echo $! > RUNNING_PID
fi
