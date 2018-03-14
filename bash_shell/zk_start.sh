#!/bin/bash
echo "================     开始启动所有节点服务            ==========="
echo "================    正在启动Zookeeper                ==========="
for i in hadoop@hadoop200 hadoop@hadoop201 hadoop@hadoop202
do
	echo "=============== $i ================"
	ssh $i '/opt/module/zookeeper-3.4.10/bin/zkServer.sh start'
done
