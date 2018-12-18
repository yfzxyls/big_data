package com.soap.storm.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * @author yangfuzhao on 2018/12/8.
 */
public class SumSplitBolt extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String[] s = tuple.getString(0).split(" ");
        collector.emit(new Values(s[0], Integer.parseInt(s[1])));
    }
}
