package com.soap.storm.trident;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;

import java.util.Map;

/**
 * @author yangfuzhao on 2018/12/21.
 */
public class SentenceTridentSpout implements ITridentSpout {
    @Override
    public BatchCoordinator getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return null;
    }

    @Override
    public Emitter getEmitter(String txStateId, Map conf, TopologyContext context) {
        return null;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return null;
    }
}
