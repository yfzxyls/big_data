package com.soap.hdfs.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by yangf on 2018/3/12.
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {


    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
        long sum_upFlow = 0;
        long sum_downFlow = 0;
        for (FlowBean flowBean : values) {
            sum_upFlow += flowBean.getSumFlow();
            sum_downFlow += flowBean.getDownFlow();
        }
        FlowBean resultBean = new FlowBean(sum_upFlow, sum_downFlow);
        context.write(key, resultBean);
    }
}
