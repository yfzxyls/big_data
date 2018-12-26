package com.soap.storm.trident.diagonsis;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;

/**
 * @author yangfuzhao on 2018/12/25.
 */
public class OutbreakTrendFactory implements StateFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new OutbreakTrendState(new OutbreakTrendBackingMap());
    }
}
