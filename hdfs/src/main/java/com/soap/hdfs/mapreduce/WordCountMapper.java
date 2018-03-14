package com.soap.hdfs.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yangf on 2018/3/11.
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);
    }


    private Text key = new Text();
    private IntWritable value = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String lineStr = value.toString();
        String[] split = lineStr.split("\t");
        for (int i = 0; i < split.length; i++) {
            String s = split[i];
            this.key.set(s);
            this.value.set(1);
            context.write(this.key,this.value);
        }
    }

    @Override
    protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    public void run(Mapper.Context context) throws IOException, InterruptedException {
        super.run(context);
    }

}
