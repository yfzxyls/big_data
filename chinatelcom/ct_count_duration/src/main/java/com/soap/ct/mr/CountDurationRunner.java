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
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.Enumeration;

/**
 * Created by soap on 2018/3/23.
 */
public class CountDurationRunner implements Tool {

    private Configuration conf = null;

    @Override
    public int run(String[] strings) throws Exception {
         
        addJarToClassPath();

        Job job = Job.getInstance(conf, getClass().getSimpleName());
        job.setJarByClass(CountDurationRunner.class);
        //添加第三方依赖
        job.addFileToClassPath(new Path("/jobs/mysql-connector-java-5.1.27-bin.jar"));
        job.addFileToClassPath(new Path("/jobs/fastjson-1.2.36.jar"));
//        addTmpJars("/jobs/ct_count_duration-1.0-SNAPSHOT.jar", conf);
//        addTmpJars("/jobs/mysql-connector-java-5.1.27-bin.jar", conf);
//     
        String tableName = "ns_ct:tb_calllog";
        //04_19775072523_20170104->04_19775072523_20170201
//        String start = "04_19775072523_20170104";
//        String stop = "04_19775072523_20170201";
//        Scan scan = new Scan().addFamily(Bytes.toBytes("f1")).setStartRow(Bytes.toBytes(start)).setStopRow(Bytes.toBytes(stop));
        //TODO:先测试少量数据
        Scan scan = new Scan().addFamily(Bytes.toBytes("f1"));
        if (strings != null && strings.length >= 2) {
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

    /**
     * 加载当前 Class 中jar包到当前虚拟机
     * @throws NoSuchMethodException
     * @throws MalformedURLException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    private void addJarToClassPath() throws NoSuchMethodException, MalformedURLException, IllegalAccessException, InvocationTargetException {
        Method addURL = URLClassLoader.class.getDeclaredMethod("addURL", new Class[]{URL.class});
        addURL.setAccessible(true);
        URLClassLoader classloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        String url = findContainingJar(getClass());
        //        String url = "file:///home/hadoop/calllog/jobs/lib/ct_count_duration-1.0-SNAPSHOT.jar"; // 包路径位置
        URL classUrl = new URL(url);
        addURL.invoke(classloader, new Object[]{classUrl});
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
        String tmpJars = conf.get(MRJobConfig.CLASSPATH_FILES);
        if (tmpJars == null || tmpJars.length() == 0) {
            conf.set(MRJobConfig.CLASSPATH_FILES, newJarPath);
        } else {
            conf.set(MRJobConfig.CLASSPATH_FILES, tmpJars + "," + newJarPath);
        }
    }

    /**
     * 根据 Class 找出jar包位置
     * @param clazz
     * @return
     */
    public static String findContainingJar(Class<?> clazz) {
        ClassLoader loader = clazz.getClassLoader();
        String classFile = clazz.getName().replaceAll("\\.", "/") + ".class";
        try {
            Enumeration itr = loader.getResources(classFile);
            URL url;
            do {
                if(!itr.hasMoreElements()) {
                    return null;
                }
                url = (URL)itr.nextElement();
            } while(!"jar".equals(url.getProtocol()));
            String toReturn = url.getPath();
//            if(toReturn.startsWith("file:")) {
//                toReturn = toReturn.substring("file:".length());
//            }
            toReturn = URLDecoder.decode(toReturn, "UTF-8");
            return toReturn.replaceAll("!.*$", "");
        } catch (IOException var6) {
            throw new RuntimeException(var6);
        }
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new CountDurationRunner(), args);
        System.exit(status);
    }
}
