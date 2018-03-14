package com.soap.hdfs.writable;

import com.soap.hdfs.utils.Tools;
import javafx.scene.text.Text;
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

            Tools.setInput(job, "C:\\Users\\yangf\\Desktop\\phone_data.txt");

            Tools.setOutPut(job, "C:\\Users\\yangf\\Desktop\\flow_output");
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
