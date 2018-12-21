package com.soap.storm.core;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * @author yangfuzhao on 2018/12/7.
 */
public class MyBaseFunc extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

    }
}
