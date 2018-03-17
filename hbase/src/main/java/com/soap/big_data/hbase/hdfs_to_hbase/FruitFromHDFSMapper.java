package com.soap.big_data.hbase.hdfs_to_hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yangf on 2018/3/16.
 */
public class FruitFromHDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(FruitFromHDFSMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String rowKey = fields[0];
        String name = fields[1];
        String color = fields[2];
        LOG.debug("load date : " + "rowKey = " + rowKey + ", name = " + name + ",color = " + color);
        ImmutableBytesWritable ibw = new ImmutableBytesWritable(Bytes.toBytes(rowKey));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(color));
        context.write(ibw, put);
    }
}
