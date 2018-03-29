package com.soap.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Created by yangf on 2018/3/6.
 */
public class HdfsClient {

    FileSystem fs = null;

    @Before
    public void getConf() throws Exception {
        Configuration conf = new Configuration();
        //conf.set("dfs.replication", "2");
        fs = FileSystem.get(new URI("hdfs://hadoop200:9000"), conf, "hadoop");
    }

    /**
     * 创建文件夹
     *
     * @throws Exception
     */
    @Test
    public void testMkdirs() throws Exception {
        try {
            fs.mkdirs(new Path("/user/client"));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    /**
     * 将会覆盖已有文件     参数优先级：代码中设置 > classpath中的配置 > 文件系统配置
     *
     * @throws IOException
     */
    @Test
    public void copyFromLocalToFile() throws IOException {
        fs.copyFromLocalFile(
                new Path("C:\\Users\\yangf\\Desktop\\phone_data.txt"),
                new Path("/writable"));
    }

    /**
     * 文件下载 可选参数
     * @throws IOException
     */

    @Test
    public void copyFileToLocal() throws IOException {
        //fs.copyToLocalFile(new Path("/user/client/test/upload.txt"), new Path("C:\\Users\\yangf\\Downloads\\downlod1.txt"));
        //true将会删除hdfs中的文件
       // fs.copyToLocalFile(true,new Path("/user/client/test/upload.txt"), new Path("C:\\Users\\yangf\\Downloads\\downlod1.txt"));
        //.downlod1.txt.crc 6372 6300 0000 0200 f017 6316 
        fs.copyToLocalFile(false,new Path("/user/client/test/upload.txt"), new Path("C:\\Users\\yangf\\Downloads\\downlodRaw.txt"),true);
    }

    @Test
    public void testListFile() throws IOException {
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator =
                fs.listFiles(new Path("/user"), true);
        while (fileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus status = fileStatusRemoteIterator.next();
            //获取文件名
            System.out.println(status.getPath().getName());
            //获取文件长度
            System.out.println(status.getLen());
            System.out.println(status.getPermission());
            // z组
            System.out.println(status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            
        }
    }

    @Test
    public void testDeleteFile() throws IOException{
         fs.deleteOnExit(new Path("/flow_output/writable.txt"));
    }

    /**
     * 使用流上传至文件系统
     * @throws IOException
     */
    @Test
    public void testIOToHDFS() throws IOException{
        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new java.io.File("C:\\Users\\yangf\\Downloads\\upload.txt"));

        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/user/client/test/file_out_put.txt"));

        // 4 流对拷
        IOUtils.copyBytes(fis, fos,1024);

        // 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }

    @Test
    public void testIOToLcoal() throws IOException{
        // 1 创建输出
        FileOutputStream fis = new FileOutputStream(new java.io.File("C:\\Users\\yangf\\Downloads\\download.txt"));

        // 3 获取输出流
        FSDataInputStream fsDataInputStream = fs.open(new Path(""));

        // 4 流对拷
        IOUtils.copyBytes(fsDataInputStream, fis,1024);

        // 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fsDataInputStream);
    }


    @After
    public void close() {
        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
