package com.feizao.study.youtube_etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.xml.soap.Text;
import java.io.IOException;

/**
 * Created by yangf on 2018/3/9.
 */
public class VideoETLRunner implements Tool {
    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        conf.set("input_path", args[0]);
        conf.set("output_path", args[1]);
        Job job = Job.getInstance(conf, "elt");
        job.setJarByClass(VideoETLRunner.class);
        job.setMapperClass(VideoETLMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        initJobInputPath(job);
        initJobOutputPath(job);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void initJobOutputPath(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        String outPathString = conf.get("output_path");

        FileSystem fs = FileSystem.get(conf);

        Path outPath = new Path(outPathString);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        FileOutputFormat.setOutputPath(job, outPath);

    }

    private void initJobInputPath(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        String inPathString = conf.get("input_path");

        FileSystem fs = FileSystem.get(conf);

        Path inPath = new Path(inPathString);
        if (fs.exists(inPath)) {
            FileInputFormat.addInputPath(job, inPath);
        } else {
            throw new RuntimeException("HDFS中该文件目录不存在：" + inPathString);
        }
    }


    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) {
        if (args == null || args.length < 2) {
            System.out.println("参数");
            return;
        }
        try {
            int resultCode = ToolRunner.run(new VideoETLRunner(), args);
            if (resultCode == 0) {
                System.out.println("Success!");
            } else {
                System.out.println("Fail!");
            }
            System.exit(resultCode);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
