package com.soap.storm.trident.diagonsis;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;

import java.util.Map;

/**
 * @author yangfuzhao on 2018/12/21.
 */
public class DiagonisiEventSpout implements ITridentSpout<Long> {

    BatchCoordinator<Long> coordinator = new DefaultCoordinator();
    Emitter<Long> emitter = new DiagnosisEventEmitter();


    @Override
    public BatchCoordinator<Long> getCoordinator(String txStateId, Map conf, TopologyContext context) {
        return coordinator;
    }

    @Override
    public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {

        return emitter;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("event");
    }




}
