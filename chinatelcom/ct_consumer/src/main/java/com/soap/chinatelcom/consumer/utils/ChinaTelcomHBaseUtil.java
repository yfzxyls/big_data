package com.soap.chinatelcom.consumer.utils;

import com.soap.big_data.hbase.util.HBaseUtil;
import com.soap.chinatelcom.consumer.constant.HBaseConstant;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;


/**
 * Created by yangf on 2018/3/20.
 */
public class ChinaTelcomHBaseUtil {

    public static boolean isTableExist(Connection connection, String tableName) {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
           return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.close(admin);
        }
        return false;
    }

    public static void createNamespace(Connection connection, String namespace) {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.close(admin);
        }
    }

    /**
     * 创建预分区表
     *
     * @param connection
     * @param tableName
     * @param regions
     * @param columnFamily
     * @param maxVersion
     */
    public static void createRegionTable(Connection connection, String tableName, int regions, String[] columnFamily, int maxVersion) {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String column : columnFamily) {
                hTableDescriptor.addFamily(new HColumnDescriptor(column));
            }
            //TODO 建表时增加协处理器
            hTableDescriptor.addCoprocessor("com.soap.chinatelcom.consumer.coprocessor.CalleeCoprocessor");
            admin.createTable(hTableDescriptor, genSplits(regions));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.close(admin);
        }
    }

    /**
     * 生成分区健
     *
     * @param regions
     * @return
     */
    public static byte[][] genSplits(int regions) {

        DecimalFormat decimalFormat = new DecimalFormat("00");
        TreeSet<byte[]> regionTree = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < regions; i++) {
            regionTree.add(Bytes.toBytes(decimalFormat.format(i) + "|"));
        }
        byte[][] splitKey = new byte[regionTree.size()][];

        Iterator<byte[]> iterator = regionTree.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            splitKey[i++] = iterator.next();
        }
        return splitKey;
    }


    /**
     * 根据主叫 通话时间 被叫 标志 通话时长 生成rowkey
     *
     * @return regionCode_call1_buildTime_call2_flag_duration
     */
    public static String genRowKey(String regionCode, String call1,
                                   String buildTime,
                                   String call2,
                                   String flag,
                                   String duration) {
        StringBuilder sb = new StringBuilder();
        sb.append(regionCode + HBaseConstant.HBASE_ROW_KEY_SPLIT)
                .append(call1 + HBaseConstant.HBASE_ROW_KEY_SPLIT)
                .append(buildTime + HBaseConstant.HBASE_ROW_KEY_SPLIT)
                .append(call2 + HBaseConstant.HBASE_ROW_KEY_SPLIT)
                .append(flag + HBaseConstant.HBASE_ROW_KEY_SPLIT)
                .append(duration);
        return sb.toString();
    }


    public static String genRegionCode(String call, String buildTime, int regions) {
        //取出电话号码的后4位
        String last4Num = call.substring(call.length() - 4);
        //取出年月
        String first4Num = buildTime.replaceAll("-", "").substring(0, 6);
        int hashCode = (Integer.valueOf(last4Num) ^ Integer.valueOf(first4Num)) % regions;
        return new DecimalFormat("00").format(hashCode);
    }


    /**
     * 累积100条数据后提交
     *
     * @param putList
     * @param table
     */
    public static void put(List<Put> putList, Table table) {
        HTable hTable = null;
        try {
            hTable = (HTable) table;
            hTable.setAutoFlushTo(false);
            hTable.setWriteBufferSize(1024 * 1024);
            hTable.put(putList);
            hTable.flushCommits();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (hTable != null) {
                    hTable.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
