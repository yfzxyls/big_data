package com.soap.storm.window;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.Map;

/**
 * @author yangfuzhao on 2018/12/26.
 */
public class SlidingWindowBolt extends BaseWindowedBolt {
    private OutputCollector collector;

    // 接受tuple前调用
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    // 在每一次窗口激活后调用，如到达用户时间间隔或者满足tuple数量
    @Override
    public void execute(TupleWindow inputWindow) {
        long count = 0;
        for (Tuple tuple : inputWindow.get()) {
            // do the windowing computation
            count++;
            System.out.println(tuple);
        }
        System.out.println(count);
        collector.emit(new Values(count));
        // emit the results
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }
}
