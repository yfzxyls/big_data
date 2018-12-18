package com.soap.storm.trident;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * @author yangfuzhao on 2018/12/8.
 */
public class FilterFunc extends BaseFilter {

    public boolean isKeep(TridentTuple tuple) {
        String string = tuple.getString(0);
        if(StringUtils.equals(string,"the"))return false;
        return true;
    }
}
