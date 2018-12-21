package com.soap.storm.trident;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * @author yangfuzhao on 2018/12/8.
 */
public class PrintFunc extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
//        System.out.println(tuple.size());
        System.out.println("Thread:"+ Thread.currentThread().getName() + tuple.get(0) +"->" + tuple.get(1));
//        System.out.println("Thread:"+ Thread.currentThread().getName() + tuple.get(0));
    }
}
