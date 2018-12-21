package com.soap.storm.core;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 自动 ack
 */
public class SplitBoltBase implements IBasicBolt {


    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // 获取key value对后，取出value值
        String values = input.getStringByField("sentence");
        if (StringUtils.isNotBlank(values)) {
            // 按空格分割value
            String[] valueArr = values.split(" ");
            for (String value : valueArr) {
                // 向后面的组件发射封装好的tuple
                collector.emit(new Values(value));
            }
        }
    }

    @Override
    public void cleanup() {

    }

    /**
     * 设置key名称，接受时需相同
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        arg0.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
