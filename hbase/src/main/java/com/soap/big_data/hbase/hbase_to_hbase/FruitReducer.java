package com.soap.big_data.hbase.hbase_to_hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Created by yangf on 2018/3/16.
 */
public class FruitReducer extends TableReducer<ImmutableBytesWritable,Put,NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for(Put put : values){
            context.write(NullWritable.get(),put);
            //System.out.println("put :" +  put.getRow());
        }
    }
}
