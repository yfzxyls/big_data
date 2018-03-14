package com.soap.big_data.hbase;

import com.soap.big_data.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Created by yangf on 2018/3/14.
 */
public class HBaseClient {

    public static void main(String[] args) throws Exception {
//        HBaseConfiguration conf = new HBaseConfiguration();
        Configuration conf = HBaseConfiguration.create();
        String tableName = "staff";
       // System.out.println(HBaseUtil.isTableExist(conf, tableName));
        String column = "info";

       // System.out.println("创建表 " + HBaseUtil.createTable(conf,tableName,column));
       // System.out.println(HBaseUtil.isTableExist(conf, tableName));
        System.out.println("删除表：" + HBaseUtil.dropTable(conf,tableName,true));

    }
}
