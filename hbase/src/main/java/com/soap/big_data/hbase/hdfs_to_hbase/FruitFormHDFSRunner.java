package com.soap.big_data.hbase.hdfs_to_hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by yangf on 2018/3/16.
 */
public class FruitFormHDFSRunner implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());
        job.setMapperClass(FruitFromHDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        Path inPath = new Path("hdfs://hadoop200:9000/input_hbase/fruit.tsv");
        FileInputFormat.addInputPath(job, inPath);
        TableMapReduceUtil.initTableReducerJob("fruit_mr", FruitFromHDFSReducer.class, job);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int status = ToolRunner.run(conf, new FruitFormHDFSRunner(), args);
        System.exit(status);
    }
}
