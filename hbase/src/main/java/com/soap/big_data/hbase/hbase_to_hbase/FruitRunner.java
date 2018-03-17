package com.soap.big_data.hbase.hbase_to_hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by yangf on 2018/3/16.
 */
public class FruitRunner implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(conf);

        job.setJarByClass(getClass());
        TableMapReduceUtil.initTableMapperJob(
                "fruit", new Scan(),
                FruitMapper.class, ImmutableBytesWritable.class, Put.class, job);
        TableMapReduceUtil.initTableReducerJob("fruit_mr",
                FruitReducer.class,
                job);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int status = ToolRunner.run(conf,new FruitRunner(), args);
        System.exit(status);
    }
}
