package com.soap.hdfs.group_comparator;

import com.soap.hdfs.partition.OrderPartitioner;
import com.soap.hdfs.utils.Tools;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;

/**
 * Created by yangf on 2018/3/15.
 */
public class OrderSortRunner {
    public static void main(String[] args) {
        try {
            Job job = Tools.getJob();
            job.setJarByClass(OrderSortRunner.class);

            Tools.setMapper(job, OrderSortMapper.class, OrderBean.class, NullWritable.class);
            Tools.setReduce(job, OrderSortReducer.class, OrderBean.class, NullWritable.class);

            Tools.setInput(job, "D:\\study\\data\\input\\GroupingComparator.txt");
            Tools.setOutput(job, "D:\\study\\data\\output\\GroupingComparator2");
            // 10 设置reduce端的分组
            job.setGroupingComparatorClass(OrderGroupingComparator.class);
            // 7 设置分区
            job.setPartitionerClass(OrderPartitioner.class);
            // 8 设置reduce个数
            job.setNumReduceTasks(3);
            System.out.println(job.waitForCompletion(true));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
