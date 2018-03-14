#!/bin/bash
echo "================     开始启动hbase服务            ==========="
ssh hadoop200 '/opt/module/hbase-1.3.1/bin/start-hbase.sh'
#ssh hadoop200 '/opt/module/hbase-1.3.1/bin/hbase-daemon.sh start master'	
#for i in hadoop200 hadoop201 hadoop202
#do
#	echo "================ $i ============="
#	ssh $i '/opt/module/hbase-1.3.1/bin/hbase-daemon.sh start regionserver'
#done
