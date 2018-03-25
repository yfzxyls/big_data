package com.soap.ct.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by yangf on 2018/3/11.
 */
public class Tools {

//    static {
//        configuration.set("fs.defaultFS", "hdfs://hadoop200:9000");
//        System.setProperty("HADOOP_USER_NAME", "hadoop");
//    }

    public static Job getJob() {
        Configuration configuration = new Configuration();
        Job job = null;
        try {
            job = Job.getInstance(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return job;
    }

    public static void setMapper(Job job, Class mapperClass, Class keyClass, Class valueClass) {
        job.setMapperClass(mapperClass);
        job.setMapOutputKeyClass(keyClass);
        job.setMapOutputValueClass(valueClass);
    }

    public static void setReduce(Job job, Class reduceClass, Class keyClass, Class valueClass) {
        job.setReducerClass(reduceClass);
        job.setOutputKeyClass(keyClass);
        job.setOutputValueClass(valueClass);
    }

    public static void setInput(Job job, String path) {
        try {
            FileInputFormat.addInputPath(job, new Path(path));
            FileInputFormat.setMaxInputSplitSize(job,1024);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void setOutput(Job job, String path) {
        try {
            FileOutputFormat.setOutputPath(job, new Path(path));
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }

    public static void setInputFormatAndSplitSize(Job job, Class<? extends InputFormat> cls, int maxSize, int minSize1) {
        job.setInputFormatClass(cls);
        CombineTextInputFormat.setMaxInputSplitSize(job, maxSize);
        CombineTextInputFormat.setMinInputSplitSize(job, minSize1);
    }

    /**
     * 指定按照 分割符读取行尾key value 形式 key 为分隔符前字符 value 为分隔符后的字符
     * @param job
     * @param keyValueTextInputFormatClass
     * @param splitStr
     */
    public static void setInputKeyValueFormat(Job job, Class<KeyValueTextInputFormat> keyValueTextInputFormatClass, String splitStr) {
        job.setInputFormatClass(keyValueTextInputFormatClass);
        job.getConfiguration().set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,splitStr);
    }

    public static void setPartition(Job job,Class<? extends Partitioner> clz, int numPartitions) {
        job.setPartitionerClass(clz);
        job.setNumReduceTasks(numPartitions);
    }
}
