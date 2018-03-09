package com.feizao.study.youtube_etl;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yangf on 2018/3/9.
 */
public class VideoETLMapper extends Mapper<Object, Text, NullWritable, Text> {
    private Text text = new Text();

    protected void map(Object obj, Text value, Context context) throws IOException, InterruptedException {
        String videoETL = ETLUtil.videoETL(value.toString());
        if (StringUtils.isBlank(videoETL)) return;
        text.set(videoETL);
        context.write(NullWritable.get(), text);
    }
}
