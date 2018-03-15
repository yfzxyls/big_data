package com.soap.hdfs.partition;

import com.soap.hdfs.group_comparator.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by yangf on 2018/3/15.
 */
public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        return (orderBean.getOrder_id() & Integer.MAX_VALUE) % numPartitions;
    }
}
