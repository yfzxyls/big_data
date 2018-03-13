package com.feizao.hdfs.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by yangf on 2018/3/12.
 */
public class FlowBean implements Writable,Comparable<FlowBean> {

    private long upFlow;
    private long downFlow;

    private long sumFlow;

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;                   
        this.sumFlow = upFlow + downFlow;
    }

    //无参构造器用于反射时创建对象
    public FlowBean() {
    }


    /**
     * 重写 序列化方法
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 重写 反序列 方法
     * 注意反序列化的顺序和序列化的顺序完全一致
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    /**
     * 如果需要将自定义的bean放在key中传输，
     * 则还需要实现comparable接口，
     * 因为mapreduce框中的shuffle过程一定会对key进行排序
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowBean o) {
        // 倒序排列，从大到小
        return this.sumFlow > o.getSumFlow() ? -1 : 1;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }


}
