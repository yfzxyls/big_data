package com.soap.storm.trident.diagonsis;

import com.google.common.collect.Lists;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yangfuzhao on 2018/12/25.
 */
public class HourAssignment extends BaseFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(HourAssignment.class);
    }

    /**
     * 以城市： diagnosisCode ： 小时作为key增加一个字段
     * @param tuple
     * @param collector
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        DiagnosisEvent diagnosis = (DiagnosisEvent) tuple.getValue(0);
        String city = (String) tuple.getValue(1);
        long timestamp = diagnosis.time;
        long hourSinceEpoch = timestamp / 1000 / 60 / 60;
        LOG.debug("Key =  [" + city + ":" + hourSinceEpoch + "]");
        String key = city + ":" + diagnosis.diagnosisCode + ":" + hourSinceEpoch;


        List<Object> values = Lists.newArrayList();
        values.add(hourSinceEpoch);
        values.add(key);
        collector.emit(values);
    }
}
