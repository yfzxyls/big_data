#!/bin/bash
echo "================     开始停止所有节点服务            ==========="
echo "================ hadoop201节点正在停止JobHistoryServer ==========="
ssh hadoop@hadoop201 '/opt/module/hadoop-2.7.2/sbin/mr-jobhistory-daemon.sh stop historyserver'
echo "================    正在停止YARN                     ==========="
ssh hadoop@hadoop201 '/opt/module/hadoop-2.7.2/sbin/stop-yarn.sh'
echo "================    正在停止HDFS                     ==========="
ssh hadoop@hadoop200 '/opt/module/hadoop-2.7.2/sbin/stop-dfs.sh'
