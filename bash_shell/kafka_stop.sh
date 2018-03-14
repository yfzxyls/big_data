#!/bin/bash
echo "================     开始启动所有节点服务            ==========="
echo "================    正在启动kafka              ==========="
for i in hadoop@hadoop200 hadoop@hadoop201 hadoop@hadoop202
do	
	echo "====================== $i kafka ===================="
	ssh $i '/opt/module/kafka_2.11-0.11.0.2/bin/kafka-server-stop.sh'
done
