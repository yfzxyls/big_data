package com.soap.ct.convert;

import com.soap.ct.writable.BaseDimensionWritable;

/**
 * Created by soap on 2018/3/24.
 */
public interface IConvert {

    /**
     * 根据不同维度 从内存缓存询，若没有从数据库查询，没有则插入数据并返回id ,将id 放入内存缓存
     * @param baseDimensionWritable
     * @return
     */
    public int getDimensionId(BaseDimensionWritable baseDimensionWritable);
}
