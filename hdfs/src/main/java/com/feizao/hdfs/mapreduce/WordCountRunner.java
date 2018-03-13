package com.feizao.hdfs.mapreduce;

import com.feizao.hdfs.utils.Tools;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * Created by yangf on 2018/3/11.
 */
public class WordCountRunner{

    public static void main(String[] args) {
       
        try {
            Job job = Tools.getJob();

            job.setJarByClass(WordCountRunner.class);

            Tools.setMapper(job, WordCountMapper.class, LongWritable.class, Text.class);

            Tools.setReduce(job, WordCountReduce.class, Text.class, IntWritable.class);

            Tools.setInput(job, "/user/hive/warehouse/emp/emp.txt");

            Tools.setOutPut(job, "/user/output/wordcount" );

            System.out.println(job.waitForCompletion(true));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
