package com.soap.big_data.hbase.hdfs_to_hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Created by yangf on 2018/3/16.
 */
public class FruitFromHDFSReducer extends TableReducer<ImmutableBytesWritable,Put,NullWritable> {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(FruitFromHDFSMapper.class);
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        
        for (Put put : values){
            context.write(NullWritable.get(),put);
            LOG.debug("reduce : row " + Bytes.toString(put.getRow()) +
                    "columnFamily :" + put.get(Bytes.toBytes("info"),Bytes.toBytes("name")).toString());
        }

    }
}
