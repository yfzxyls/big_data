package com.soap.storm.trident;

import com.google.common.collect.Maps;
import com.soap.storm.trident.diagonsis.OutbreakTrendBackingMap;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @author yangfuzhao on 2018/12/26.
 *
 * 模糊事务型
 */
public class OpaqueMapStateFactory implements StateFactory {

    private static final Logger LOG = LoggerFactory.getLogger(OpaqueMapStateFactory.class);
    Map<String, OpaqueValue> storage = Maps.newConcurrentMap();
//    AtomicLong tx = new AtomicLong(0);

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        IBackingMap<OpaqueValue> iBackingMap = new IBackingMap<OpaqueValue>() {
            @Override
            public List<OpaqueValue> multiGet(List<List<Object>> keys) {
                List<OpaqueValue> values = new ArrayList<>();
                for (List<Object> key : keys) {
                    OpaqueValue value = storage.get(key.get(0));
                    if (value == null) {
                        values.add(new OpaqueValue(1L,0L,0L));
                    } else {
                        values.add(value);
                    }
                }
                return values;
            }

            @Override
            public void multiPut(List<List<Object>> keys, List<OpaqueValue> vals) {
                for (int i = 0; i < keys.size(); i++) {
                    LOG.info("Persisting [" + keys.get(i).get(0) + "] ==> [" + vals.get(i) + "]");
                    storage.put((String) keys.get(i).get(0), vals.get(i));
                }
            }
        };
        return OpaqueMap.build(iBackingMap);
    }
}
