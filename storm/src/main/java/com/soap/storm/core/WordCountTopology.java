package com.soap.storm.core;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {

    private static final String SPOUT_ID = "sentenceSpout";
    private static final String SPLIT_BOLT = "splitBolt";
    private static final String COUNT_BOLT = "countBolt";
    private static final String PRINT_BOLT = "printBolt";

    /**
     * bolt、spout 组件
     * 1.task数据=组件的数量
     * 2.默认一个线程执行一个组件，多个组件可以共用线程,组件的值独立
     * 3.并行度设置比task多时，只会起与task一样的线程
     */

    public static void main(String[] args) throws Exception {

        // 构造Topology
        TopologyBuilder builder = new TopologyBuilder();
        // SPOUT_ID--> SPLIT_BOLT --> COUNT_BOLT --> PRINT_BOLT
        // 指定spout;
        builder.setSpout(SPOUT_ID, new SentenceSpout());
        // 指定bolt，并指定当有有多个bolt时，数据流发射的分组策略;
        // setNumTasks(2) 可以设置多task并行执行

        builder.setBolt(SPLIT_BOLT, new SplitBoltBase())
                .shuffleGrouping(SPOUT_ID);

        builder.setBolt(COUNT_BOLT, new CountBolt(), 2)
                .fieldsGrouping(SPLIT_BOLT, new Fields("word")).setNumTasks(2);
//                .shuffleGrouping(SPLIT_BOLT).setNumTasks(3);

        /**
         * 分组
         * 1.shuffle 随机分组，数据随机分发到下游，均衡
         * 2.field 按指定的字段分组，保证相同字段的数据进入同一个bolt
         */
//                .fieldsGrouping(SPLIT_BOLT, new Fields("word")).setNumTasks(1);
        // 全局分组，所有tuple发射到一个printbolt，一般是id最小的那一个
        builder.setBolt(PRINT_BOLT, new PrintBolt()).globalGrouping(COUNT_BOLT);

        Config conf = new Config();
//        可以设置多worker默认1个，进程数
        conf.setNumWorkers(2);
//        conf.setDebug(true);
//        conf.setNumAckers(0);
        // 本地执行
//        LocalCluster localCluster = new LocalCluster();
//        StormTopology topology = builder.createTopology();
////        topology.
//        localCluster.submitTopology("wordcount", conf, topology);


//        // 提交脚本： /Users/soapy/soft/apache-storm-1.2.1/bin/storm jar /Users/soapy/IdeaProjects/big_data/storm/target/storm-1.0-SNAPSHOT.jar com.soap.storm.core.WordCountTopology

        StormSubmitter.submitTopology("wordcount", conf, builder.createTopology());

//        localCluster.killTopology("wordcount");
//        localCluster.shutdown();

    }
}
