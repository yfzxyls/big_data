package com.soap.storm.trident.diagonsis;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yangfuzhao on 2018/12/25.
 */
public class OutbreakDetector extends BaseFunction {
    private static final long serialVersionUID = 1L;
    public static final int THRESHOLD = 10000;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String key = (String) tuple.getValue(0);
        Long count = (Long) tuple.getValue(1);
        if (count > THRESHOLD) {
            List<Object> values = new ArrayList<Object>();
            values.add("Outbreak detected for [" + key + "]!");
            collector.emit(values);
        }
    }
}
