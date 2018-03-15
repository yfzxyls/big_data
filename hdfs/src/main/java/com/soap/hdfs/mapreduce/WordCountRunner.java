package com.soap.hdfs.mapreduce;

import com.soap.hdfs.combiner.WorldCountCombiner;
import com.soap.hdfs.utils.Tools;
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

            Tools.setInput(job, "D:\\study\\data\\input\\combiner.txt");
            Tools.setOutput(job, "D:\\study\\data\\output\\combiner1" );

            //使用combiner合并
            job.setCombinerClass(WorldCountCombiner.class);

            System.out.println(job.waitForCompletion(true));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
