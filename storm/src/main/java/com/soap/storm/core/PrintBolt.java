package com.soap.storm.core;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class PrintBolt extends BaseRichBolt {

    OutputCollector collector = null;

    @Override
    public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
        collector = arg2;
    }

    /**
     * 打印到控制台
     */
    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        int count = input.getIntegerByField("count");
        System.out.println(word + " ：" + count);
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {

    }

    @Override
    public void cleanup() {
        super.cleanup();
        System.out.println("cleanup\n");
    }
}

