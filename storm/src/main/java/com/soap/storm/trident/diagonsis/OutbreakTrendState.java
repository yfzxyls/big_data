package com.soap.storm.trident.diagonsis;

import org.apache.storm.trident.state.map.NonTransactionalMap;

/**
 * @author yangfuzhao on 2018/12/25.
 */
public class OutbreakTrendState extends NonTransactionalMap<Long> {
    protected OutbreakTrendState(OutbreakTrendBackingMap outbreakBackingMap) {
        super(outbreakBackingMap);
    }
}
