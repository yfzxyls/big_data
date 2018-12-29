package com.soap.storm.window;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @author yangfuzhao on 2018/12/26.
 */
public class PrintBolt implements IBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.err.println("初始化。。。PrintBolt");
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        int size = input.size();
        for ( int i = 0;i<size;i++){
            Object value = input.getValue(i);
            System.out.println(" count："+value);
        }
    }

    @Override
    public void cleanup() {
        System.out.println("cleanup\n");
    }
}
