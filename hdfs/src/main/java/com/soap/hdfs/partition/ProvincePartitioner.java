package com.soap.hdfs.partition;


import com.soap.hdfs.writable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by yangf on 2018/3/15.
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {

        String num = text.toString().substring(0, 3);
//        int partion = 0;
//        partion = Integer.valueOf(num) % numPartitions;
        return Integer.valueOf(num) & Integer.MAX_VALUE % numPartitions;
    }
}
