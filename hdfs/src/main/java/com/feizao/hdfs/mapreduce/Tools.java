package com.feizao.hdfs.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by yangf on 2018/3/11.
 */
public class Tools {
    public static final Configuration configuration = new Configuration();

    static {
        configuration.set("fs.defaultFS", "hdfs://hadoop200:9000");
        System.setProperty("HADOOP_USER_NAME", "hadoop");
    }

    public static Job getJob() {
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
        job.setMapOutputKeyClass(keyClass);
        job.setMapOutputValueClass(valueClass);
    }

    public static void setInput(Job job, String path) {
        try {
            FileInputFormat.addInputPath(job, new Path(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void setOutPut(Job job, String path) {
        try {
            FileOutputFormat.setOutputPath(job, new Path(path));
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }
}
