#!/bin/bash
echo "================     开始启动所有节点服务            ==========="
echo "================    正在启动HDFS                     ==========="
ssh hadoop@hadoop200 '/opt/module/hadoop-2.7.2/sbin/start-dfs.sh'
echo "================   hadoop201 正在启动YARN                     ==========="
ssh hadoop@hadoop201 '/opt/module/hadoop-2.7.2/sbin/start-yarn.sh'
echo "================ hadoop201节点正在启动JobHistoryServer ==========="
ssh hadoop@hadoop201 '/opt/module/hadoop-2.7.2/sbin/mr-jobhistory-daemon.sh start historyserver'
echo "===============hadoop200 启动 spark "
ssh hadoop@hadoop200 '/opt/module/spark-2.1.1-bin-hadoop2.7/sbin/start-all.sh'
echo "===============hadoop201 启动 JobHistory "
ssh hadoop@hadoop201 '/opt/module/spark-2.1.1-bin-hadoop2.7/sbin/start-history-server.sh'
