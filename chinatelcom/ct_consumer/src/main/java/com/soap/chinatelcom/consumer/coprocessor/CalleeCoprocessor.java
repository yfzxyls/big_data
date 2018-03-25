package com.soap.chinatelcom.consumer.coprocessor;

import com.soap.chinatelcom.consumer.constant.HBaseConstant;
import com.soap.chinatelcom.consumer.dao.HBaseSaveDao;
import com.soap.chinatelcom.consumer.utils.ChinaTelcomHBaseUtil;
import com.soap.chinatelcom.consumer.utils.PropertiesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;


/**
 * Created by yangf on 2018/3/21.
 * 将通话记录主叫与被叫交换位置 标志位更改 方便使用rowkey 查询某一号码通话记录
 */
public class CalleeCoprocessor extends BaseRegionObserver {

    Logger logger = LoggerFactory.getLogger(CalleeCoprocessor.class);
    private SimpleDateFormat fullDateFormat = new SimpleDateFormat(HBaseConstant.FULL_TIME_STAMP);
    private SimpleDateFormat shortDateFormat = new SimpleDateFormat(HBaseConstant.SHORT_TIME_STAMP);

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
       // MDC.put("fileName","coprocessor.log");
        // 判断表名防止影响其他业务
        String chinaTelcomTable = PropertiesUtil.getPropertiesKey("china_telcom_table");
        String currentTable = e.getEnvironment().getRegionInfo().getTable().getNameAsString();
        if (!chinaTelcomTable.equals(currentTable)) return;
//        01_16379727058_20191120150958_14607885120_1_1178
//        0X_14607885120_20191120150958_16379727058_0_1178
        String callRowKey = Bytes.toString(put.getRow());
        String[] rowKeys = callRowKey.split(HBaseConstant.HBASE_ROW_KEY_SPLIT);
        //主被叫判断 防止 二次进入 造成死循环
        if (HBaseConstant.CALLED_LINE_FLAG.equals(rowKeys[4])) return;

        //04_14877108872_20170202024507_13668461153_1_0143
        logger.info("####################1" + callRowKey);
        //生成新的rowkey
        String calling = rowKeys[1];
        String called = rowKeys[3];
        String buildTime = rowKeys[2];
        String flag = HBaseConstant.CALLED_LINE_FLAG;
        String duration = rowKeys[5];
        int regions = Integer.valueOf(PropertiesUtil.getPropertiesKey("china_telcom_region"));
        String regionCode = ChinaTelcomHBaseUtil.genRegionCode(called, buildTime, regions);
        String rowKey = ChinaTelcomHBaseUtil.genRowKey(regionCode, called, buildTime, calling, flag, duration);
        logger.info("####################0" + rowKey);
        Put calledPut = new Put(Bytes.toBytes(rowKey));
        String buildTimeTs = null;
        try {
            buildTimeTs = fullDateFormat.format(shortDateFormat.parse(buildTime));
        } catch (ParseException e1) {
            e1.printStackTrace();
        }
        HBaseSaveDao.putColumn(calledPut, "f2", "call1", called);
        HBaseSaveDao.putColumn(calledPut, "f2", "call2", calling);
        HBaseSaveDao.putColumn(calledPut, "f2", "build_time", buildTime);
        HBaseSaveDao.putColumn(calledPut, "f2", "build_time_ts", buildTimeTs);
        HBaseSaveDao.putColumn(calledPut, "f2", "flag", flag);
        HBaseSaveDao.putColumn(calledPut, "f2", "duration", duration);
        Table table = e.getEnvironment().getTable(TableName.valueOf(chinaTelcomTable));
        table.put(calledPut);
        if (null != table) table.close();
    }
}
