package com.soap.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * @author yangfuzhao on 2018/12/7.
 */

public class TridentTp {

    /**
     * 支持的API
     * 1、分组 group by
     * 2、连接 join
     * 3、聚合 aggregation
     * 4、过滤 filter
     * 5、持久化
     * <p>
     * 实现有且只有一次消费
     * 1、为每个batch创建唯一的ID,称为事务ID。如果一个batch重试就会有完全相同的ID
     * 2、batch之间有序执行。
     *
     * @param args
     */


    public static void main(String[] args) {

        wordCount();
//        sum();
//        merge();
    }


    public static void wordCount() {
        SentenceBatchSpout spout = new SentenceBatchSpout(new Fields("sentence"), 2,
                new Values("the cow spark jumped over the moon"),
                new Values("the  storm flink spark"),
                new Values("the flink hdfs flink kafka spark"),
                new Values("the spout bolt count spark")
        );
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();

        topology.newStream("spout", spout)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .aggregate(new Count(), new Fields("count"))
                .filter(new FilterFunc())
                .each(new Fields("word", "count"), new PrintFunc(), new Fields())
                //.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(2);
        LocalCluster localCluster = new LocalCluster();
        Config config = new Config();
//        config.setDebug(true);
        config.setNumWorkers(2);
        localCluster.submitTopology("tridentWorldCount", config, topology.build());
    }

    public static void sum() {
        SentenceBatchSpout spout = new SentenceBatchSpout(new Fields("sentence"), 2,
                new Values("the 1"),
                new Values("hadoop 2"),
                new Values("storm 3"),
                new Values("flink 2"),
                new Values("kafka 2"),
                new Values("spark 1")
        );
        spout.setCycle(true);
        TridentTopology topology = new TridentTopology();
        topology.newStream("spout", spout)
                .each(new Fields("sentence"), new SumSplitBolt(), new Fields("word", "a"))
//                .groupBy(new Fields("word"))
                .aggregate(new Fields("a"), new Sum(), new Fields("num", "b"))
                .each(new Fields("num", "b"), new PrintFunc(), new Fields())
                //.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(4);
        LocalCluster localCluster = new LocalCluster();
        Config config = new Config();
        config.setNumWorkers(1);
        localCluster.submitTopology("tridentWorldCount", config, topology.build());
    }


    /**
     * 1、输出字段以第一个流为准
     */
    public static void merge() {
        SentenceBatchSpout spout = new SentenceBatchSpout(new Fields("sentence"), 2,
                new Values("the 1"),
                new Values("hadoop 2"),
                new Values("storm 3"),
                new Values("flink 2"),
                new Values("kafka 2"),
                new Values("spark 1")
        );
        spout.setCycle(true);

        SentenceBatchSpout spout1 = new SentenceBatchSpout(new Fields("sentence1"), 2,
                new Values("the 1"),
                new Values("hadoop 2"),
                new Values("storm 3"),
                new Values("flink 2"),
                new Values("kafka 2"),
                new Values("spark 1")
        );
        spout1.setCycle(true);

        TridentTopology topology = new TridentTopology();
        Stream stream1 = topology.newStream("spout1", spout);
        Stream stream2 = topology.newStream("spout2", spout1);

        topology.merge(stream2,stream1)
                .each(new Fields("sentence1"), new PrintFunc(), new Fields());

        LocalCluster localCluster = new LocalCluster();
        Config config = new Config();
        config.setNumWorkers(4);
        localCluster.submitTopology("tridentMerge", config, topology.build());
    }
}
