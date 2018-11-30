package com.soap.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SentenceSpout  extends BaseRichSpout {

    //用于存储发射的数据，如果失败将重新发射，成功则删除
    private ConcurrentHashMap<UUID,Values> pending;

    // tuple发射器
    private SpoutOutputCollector collector;

    private static final String[] SENTENCES = {
            "hadoop",
//            "flume hadoop hive spark",
//            "zookeeper yarn spark storm",
//            "storm yarn mapreduce kafka",
//            "kafka flume storm spark"
    };

    /**
     * 用于指定只针对本组件的一些特殊配置
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /**
     * spout组件的初始化方法 创建这个sentenceSpout组件实例时调用一次
     */
    public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
        // 用实例变量接收发器
        this.collector = arg2;
        this.pending = new ConcurrentHashMap<UUID, Values>();
    }

    /**
     * 声明向后面的组件发送tuple的key是什么
     */
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("sentence"));
    }

    /**
     * 1）指定tuple的value值，封装tuple后，并将其发射给后面的组件，
     * 2）会迭代式的循环调用这个方法
     */
    public void nextTuple() {
        // 从数组中随意获取一个值
        String sentence = SENTENCES[new Random().nextInt(SENTENCES.length)];
        // 指定value值并封装为tuple后,把tuple发送给后面的组件
        Values values = new Values(sentence);
        UUID msgId = UUID.randomUUID();
        this.collector.emit(values,msgId);
        this.pending.put(msgId,values);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object msgId) {
        pending.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        this.collector.emit(this.pending.get(msgId),msgId);
    }
}
