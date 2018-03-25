package com.soap.ct.utils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by soap on 2018/3/24.
 */
public class LRUCache extends LinkedHashMap<String, Integer> {
    private static final long serialVersionUID = -8859011407705925558L;
    protected int maxElements;

    public LRUCache(int maxSize) {
        super(maxSize, 0.75F, true);
        this.maxElements = maxSize;
    }

    protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
        return this.size() > this.maxElements;
    }
}
