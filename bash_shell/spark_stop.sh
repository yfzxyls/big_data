#!/bin/bash
echo "================     开始停止所有节点服务            ==========="
echo "=============停止spark history================"
ssh hadoop@hadoop201 '/opt/module/spark-2.1.1-bin-hadoop2.7/sbin/stop-history-server.sh'
echo "=================停止spark集群==========="
ssh hadoop@hadoop200 '/opt/module/spark-2.1.1-bin-hadoop2.7/sbin/stop-all.sh'
echo "================ hadoop201节点正在停止JobHistoryServer ==========="
ssh hadoop@hadoop201 '/opt/module/hadoop-2.7.2/sbin/mr-jobhistory-daemon.sh stop historyserver'
echo "================    正在停止YARN                     ==========="
ssh hadoop@hadoop201 '/opt/module/hadoop-2.7.2/sbin/stop-yarn.sh'
echo "================    正在停止HDFS                     ==========="
ssh hadoop@hadoop200 '/opt/module/hadoop-2.7.2/sbin/stop-dfs.sh'
