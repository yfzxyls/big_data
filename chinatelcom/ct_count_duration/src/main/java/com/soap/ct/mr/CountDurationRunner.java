package com.soap.ct.mr;

import com.soap.ct.utils.Tools;
import com.soap.ct.writable.CommonDimension;
import com.soap.ct.writable.CountValueDimensionWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by soap on 2018/3/23.
 */
public class CountDurationRunner implements Tool {

    private Configuration conf = null;

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(conf, getClass().getSimpleName());
        job.setJarByClass(CountDurationRunner.class);
//        addTmpJar("/jobs/mysql-connector-java-5.1.27-bin.jar",conf);
//        addTmpJar("/jobs/ct_count_duration-1.0-SNAPSHOT.jar",conf);
        job.addFileToClassPath(new Path("/jobs/mysql-connector-java-5.1.27-bin.jar"));
//        job.addFileToClassPath(new Path("/jobs/ct_count_duration-1.0-SNAPSHOT.jar"));
//        job.addArchiveToClassPath(new Path("/jobs/mysql-connector-java-5.1.27-bin.jar"));
//      ut.println("mapreduce.job.name:" + conf.get(MRJobConfig.JOB_NAME));
        //设置mapper
        String tableName = "ns_ct:tb_calllog";
        //04_19775072523_20170104->04_19775072523_20170201
//        String start = "04_19775072523_20170104";
//        String stop = "04_19775072523_20170201";
//        Scan scan = new Scan().addFamily(Bytes.toBytes("f1")).setStartRow(Bytes.toBytes(start)).setStopRow(Bytes.toBytes(stop));
        //TODO:先测试少量数据
        Scan scan = new Scan().addFamily(Bytes.toBytes("f1"));
        if(strings != null && strings.length >= 2){
            System.out.println("分区 ： " + strings[0] + "->" + strings[1]);
            scan.setStartRow(Bytes.toBytes(strings[0]));
            scan.setStopRow(Bytes.toBytes(strings[1]));
        }
        TableMapReduceUtil.initTableMapperJob(tableName,
                scan,
                CountDurationMapper.class,
                CommonDimension.class,
                Text.class,
                job,
                true);

        //设置reducer
        Tools.setReduce(job, CountDurationReducer.class, CommonDimension.class, CountValueDimensionWritable.class);

        job.setOutputFormatClass(CountDurationOutPutFormat1.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        conf = HBaseConfiguration.create();
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void addTmpJars(String jarPath, Configuration conf) throws IOException {
        System.setProperty("path.separator", ":");
        FileSystem fs = FileSystem.getLocal(conf);
        String newJarPath = new Path(jarPath).makeQualified(fs).toString();
        String tmpjars = conf.get("tmpjars");
        if (tmpjars == null || tmpjars.length() == 0) {
            conf.set("tmpjars", newJarPath);
        } else {
            conf.set("tmpjars", tmpjars + "," + newJarPath);
        }
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new CountDurationRunner(), args);
        System.exit(status);
    }
}
