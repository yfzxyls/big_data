package com.soap.hdfs.group_comparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yangf on 2018/3/15.
 */
public class OrderSortMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable> {

    OrderBean orderBean = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");

        int orderId = Integer.valueOf(fields[0]);
        double price = Double.valueOf(fields[2]);
        orderBean.setOrder_id(orderId);
        orderBean.setPrice(price);
        context.write(orderBean,NullWritable.get());
    }
}
