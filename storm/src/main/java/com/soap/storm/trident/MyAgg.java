package com.soap.storm.trident;

import com.sun.scenario.effect.impl.sw.RendererDelegate;
import org.apache.storm.trident.operation.*;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * @author yangfuzhao on 2018/12/8.
 */

/**
 * 1、发射任意数量的元组
 * 2、执行期间可以发射数据
 */
 class MyAgg1 implements Aggregator {

    /**
     * 初始化
     * @param batchId
     * @param collector
     * @return
     */
    public Object init(Object batchId, TridentCollector collector) {
        return null;
    }

    /**
     * 输入元组时调用
     * @param val
     * @param tuple
     * @param collector
     */
    public void aggregate(Object val, TridentTuple tuple, TridentCollector collector) {

    }

    /**
     *  所有元组被aggregate后调用
     * @param val
     * @param collector
     */
    public void complete(Object val, TridentCollector collector) {

    }

    /**
     * 只在启动拓扑时调用1次。如果设置了并发度，则每一个partition调用1次。
     * @param conf
     * @param context
     */
    public void prepare(Map conf, TridentOperationContext context) {

    }

    /**
     * 只在正常关闭拓扑时调用1次。如果设置了并发度，则每一个partition调用1次。
     */
    public void cleanup() {

    }
}

/**
 * 1、使用init做聚会操作
 * 2、有元素输入是做reduce操作
 */
class MyAgg2 implements ReducerAggregator{
    public Object init() {
        return null;
    }

    public Object reduce(Object curr, TridentTuple tuple) {
        return null;
    }
}

/**
 * 1、返回单输出单一元组
 * 2、聚合操作
 * 3、如果分区中没有数据返回0
 */
class MyAgg3 implements CombinerAggregator<Long>{



    public Long init(TridentTuple tuple) {
        return null;
    }

    public Long combine(Long val1, Long val2) {
        return null;
    }

    public Long zero() {
        return null;
    }
}
