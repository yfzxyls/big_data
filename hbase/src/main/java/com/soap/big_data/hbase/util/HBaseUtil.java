package com.soap.big_data.hbase.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by yangf on 2018/3/14.
 */
public class HBaseUtil {


    public static boolean isTableExist(Configuration conf, String tableName) {
        try {
            //API过时
//            HBaseAdmin admin = new HBaseAdmin(conf);
//            return admin.tableExists(tableName.getBytes());
            //Admin admin = connection.getAdmin();
            Admin admin = ConnectionFactory.createConnection(conf).getAdmin();
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


    /**
     * @param conf
     * @param tableName    表名不能重复  表存在将抛出异常
     * @param columnFamily 列族必须有
     * @return
     */
    public static boolean createTable(Configuration conf, String tableName, String... columnFamily) throws Exception {
        try {
            if (columnFamily == null || columnFamily.length <= 0) {
                return false;
            }
            Admin admin = ConnectionFactory.createConnection(conf).getAdmin();
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
                for (int i = 0; i < columnFamily.length; i++) {
                    hTableDescriptor.addFamily(new HColumnDescriptor(columnFamily[i]));
                }
                admin.createTable(hTableDescriptor);
                return true;
            } else {
                throw new Exception("table " + tableName + "is exists");
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean truncateTable(Configuration conf, String tableName, boolean preserveSplits) {
        try {
            Admin admin = ConnectionFactory.createConnection(conf).getAdmin();
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                admin.truncateTable(TableName.valueOf(tableName), preserveSplits);
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return false;
    }

    /**
     * 删除表
     * @param conf
     * @param tableName
     * @param forceDel 是否强制删除表
     * @return
     */
    public static boolean dropTable(Configuration conf, String tableName,boolean forceDel) throws Exception{
        try {
            Admin admin = ConnectionFactory.createConnection(conf).getAdmin();
            if (forceDel){
                admin.disableTable(TableName.valueOf(tableName));
            }
            admin.deleteTable(TableName.valueOf(tableName));
        }catch (TableNotDisabledException e){
            throw e;
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }





}
