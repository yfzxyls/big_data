#!/bin/bash
echo "================    Zookeeper status                ==========="
for i in hadoop@hadoop200 hadoop@hadoop201 hadoop@hadoop202
do
	echo "================== $i =============== " 
       ssh $i '/opt/module/zookeeper-3.4.10/bin/zkServer.sh status'
done
