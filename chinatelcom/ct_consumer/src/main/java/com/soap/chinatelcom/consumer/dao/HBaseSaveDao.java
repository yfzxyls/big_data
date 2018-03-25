package com.soap.chinatelcom.consumer.dao;

import com.soap.chinatelcom.consumer.kafka_consumer.ConnectionInstance;
import com.soap.chinatelcom.consumer.utils.ChinaTelcomHBaseUtil;
import com.soap.chinatelcom.consumer.utils.PropertiesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yangf on 2018/3/21.
 */
public class HBaseSaveDao {

    /**
     * 将原始数据整理为hbase中的数据
     * regionCode_call1_buildTime_call2_flag_duration
     * @param orgString
     */
    private List<Put> puts = new ArrayList<>();
    private static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHHmmss");
    private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private int regions;
    private String tableName;
    private String nameSpace ;

    public HBaseSaveDao(){
        regions =  Integer.valueOf(PropertiesUtil.getPropertiesKey("china_telcom_region"));
        tableName = PropertiesUtil.getPropertiesKey("china_telcom_table");
        nameSpace =  PropertiesUtil.getPropertiesKey("china_telcom_namespace");
        if (!ChinaTelcomHBaseUtil.isTableExist(ConnectionInstance.getConnection(),tableName)){
            ChinaTelcomHBaseUtil.createNamespace(ConnectionInstance.getConnection(),nameSpace);
            ChinaTelcomHBaseUtil.createRegionTable(ConnectionInstance.getConnection(),tableName,regions,new String[]{"f1","f2"},0);
        }
    }

    public void saveData(String orgString){
        String[] split = orgString.split(",");
        String caller = split[0];
        String callee = split[1];
        String buildTimeStr = split[2];
        String flag = "1";
        String duration = split[3];

        String regionCode = ChinaTelcomHBaseUtil.genRegionCode(caller,buildTimeStr,Integer.valueOf(PropertiesUtil.getPropertiesKey("china_telcom_region")));

        String buildTime = null;
        try {
            buildTime = sdf1.format(sdf2.parse(buildTimeStr));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String rowKey = ChinaTelcomHBaseUtil.genRowKey(regionCode,caller,buildTime,callee,flag,duration);
        Put put = new Put(Bytes.toBytes(rowKey));

        putColumn(put,"f1","call1",caller);
        putColumn(put,"f1","call2",callee);
        putColumn(put,"f1","build_time",buildTime);
        putColumn(put,"f1","build_time_ts",buildTimeStr);
        putColumn(put,"f1","flag",flag);
        putColumn(put,"f1","duration",duration);

        puts.add(put);
        if (puts.size() > 100){
            HTable hTable = null;
            try {
                hTable = (HTable) ConnectionInstance.getConnection().getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                e.printStackTrace();
            }
            ChinaTelcomHBaseUtil.put(puts,hTable);
            puts.clear();
        }
    }

    public static void putColumn(Put put, String familyName, String columnName ,String columnValue){
        put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));
    }

}
