package com.soap.storm.core;

import com.google.common.collect.Lists;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

/**
 * 手动ack
 */
public class SplitBolt implements IRichBolt {

    // bolt组件中的发射器
    private OutputCollector collector;

    @Override
    public void cleanup() {

    }

    /**
     * 设置key名称，接受时需相同
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("word"));

//        System.out.println("declareOutputFields");
    }

    /**
     * 每次接受到前面组件发送的tuple调用一次 ，封装好tuple后发射
     */
    @Override
    public void execute(Tuple input) {
        // 获取key value对后，取出value值
        String values = input.getStringByField("sentence");
        if (values != null && !"".equals(values)) {
            // 按空格分割value
            String[] valuelist = values.split(" ");
            List<Tuple> anchors = Lists.newArrayList();
            for (String value : valuelist) {
                // 向后面的组件发射封装好的tuple
                anchors.add(input);
                this.collector.emit(input,new Values(value));
            }
            //如果不ack 或者fail 内存将会被消耗殆尽
            this.collector.ack(input);
        }
    }

    /**
     * bolt组件初始化方法，只会调用一次;一般用于不可序列化对象的初始化
     */
    @Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
        this.collector = arg2;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
