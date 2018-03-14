package com.soap.hdfs.writable;

import com.soap.hdfs.utils.Tools;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by yangf on 2018/3/12.
 */
public class FlowRunner {

    public static void main(String[] args) {
        try {
            Job job = Tools.getJob();

            job.setJarByClass(FlowRunner.class);

            Tools.setMapper(job, FlowMapper.class, Text.class, FlowBean.class);
            Tools.setReduce(job, FlowReducer.class, Text.class, FlowBean.class);
            job.setReducerClass(FlowReducer.class);

            Tools.setInput(job, "/writable");

            Tools.setOutPut(job, "/flow_output/writable");
            boolean status = job.waitForCompletion(true);

            System.exit(status == true ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }
}
