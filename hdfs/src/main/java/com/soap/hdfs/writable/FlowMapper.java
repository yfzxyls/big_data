package com.soap.hdfs.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yangf on 2018/3/12.
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text key = new Text();

    private FlowBean flowBean = null;


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");


        // 取出手机号码
        String phoneNum = fields[1];
        // 取出上行流量和下行流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        flowBean.setDownFlow(downFlow);
        flowBean.setUpFlow(upFlow);
        this.key.set(phoneNum);
        context.write(this.key, flowBean);
    }
}
