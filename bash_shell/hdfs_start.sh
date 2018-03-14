#!/bin/bash
echo "================     开始启动所有节点服务            ==========="
echo "================    正在启动HDFS                     ==========="
ssh hadoop@hadoop200 '/opt/module/hadoop-2.7.2/sbin/start-dfs.sh'
echo "================   hadoop201 正在启动YARN                     ==========="
ssh hadoop@hadoop201 '/opt/module/hadoop-2.7.2/sbin/start-yarn.sh'
echo "================ hadoop201节点正在启动JobHistoryServer ==========="
ssh hadoop@hadoop201 '/opt/module/hadoop-2.7.2/sbin/mr-jobhistory-daemon.sh start historyserver'
