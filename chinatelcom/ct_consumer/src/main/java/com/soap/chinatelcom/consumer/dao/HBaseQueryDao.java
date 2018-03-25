package com.soap.chinatelcom.consumer.dao;

import com.soap.chinatelcom.consumer.constant.HBaseConstant;
import com.soap.chinatelcom.consumer.kafka_consumer.ConnectionInstance;
import com.soap.chinatelcom.consumer.utils.ChinaTelcomHBaseUtil;
import com.soap.chinatelcom.consumer.utils.PropertiesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Created by soap on 2018/3/21.
 */
public class HBaseQueryDao {

    /**
     * 根据手机号 时间区间查询通话记录
     *
     * @param phoneNum 手机号
     * @param startDate   开始时间：2017-01-01
     * @param endDate 结束时间：2017-03-01
     */
    private int regions;
    private String tableName;


    public HBaseQueryDao() {
        regions = Integer.valueOf(PropertiesUtil.getPropertiesKey("china_telcom_region"));
        tableName = PropertiesUtil.getPropertiesKey("china_telcom_table");
    }

    public void queryRecordByPhoneNumAndDate(String phoneNum, String startDate, String endDate) throws IOException {

        //生成时间区间

        List<String[]> dateSections = genRowkeyDateSection(phoneNum, startDate, endDate);

        //根据手机号 时间区间 生成 rowkey
        Table table = ConnectionInstance.getConnection().getTable(TableName.valueOf(tableName));

        for (Iterator<String[]> iterator = dateSections.iterator(); iterator.hasNext(); ) {
            String[] next = iterator.next();
            System.out.println(next[0] + "->" + next[1] + "\n");
            Scan scan = new Scan();

            //01_11381144447_20170144314144_14422_
            //01_11381144447_201701444 | 124
            //01_11381144447_20170144414144_14422_0111
            //01_11381144447_201701444
            scan.setStartRow(Bytes.toBytes(next[0]));
            scan.setStopRow(Bytes.toBytes(next[1]));
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println(Bytes.toString(result.getRow()));
            }
        }

    }

    /**
     * 生成时间区间 类似20170101 20170201
     *
     * @param startDateStr
     * @param endDateStr
     * @return
     */
    private List<String[]> genRowkeyDateSection(String phoneNum, String startDateStr, String endDateStr) {

        DateTime startDate = null;
        DateTime endDate = null;
        try {
            startDate = new DateTime(new SimpleDateFormat(HBaseConstant.SHORT_YEAR_AND_MONTH).parse(startDateStr));
            endDate = new DateTime(new SimpleDateFormat(HBaseConstant.SHORT_YEAR_AND_MONTH).parse(endDateStr));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        DateTime tmp = startDate.plusMonths(1).dayOfMonth().withMinimumValue();
        List<String[]> dateSec = new ArrayList<>();
        while (tmp.isBefore(endDate) || tmp.isEqual(endDate)) {
            String start = startDate.toString(HBaseConstant.SHORT_YEAR_AND_MONTH);
            String end = tmp.toString(HBaseConstant.SHORT_YEAR_AND_MONTH);
            String regionCode = ChinaTelcomHBaseUtil.genRegionCode(phoneNum, start, regions);
            String startRowKey = regionCode + HBaseConstant.HBASE_ROW_KEY_SPLIT
                    + phoneNum + HBaseConstant.HBASE_ROW_KEY_SPLIT
                    + start;
            String endRowKey = regionCode + HBaseConstant.HBASE_ROW_KEY_SPLIT
                    + phoneNum + HBaseConstant.HBASE_ROW_KEY_SPLIT
                    + end;
            String[] date = new String[]{startRowKey, endRowKey};
            dateSec.add(date);
            startDate = new DateTime(tmp);
            tmp = tmp.plusMonths(1).dayOfMonth().withMinimumValue();
        }

//        for (int $i = 0; $i < dateSec.size(); $i++) {
//            String regionCode = ChinaTelcomHBaseUtil.genRegionCode(phoneNum, dateSec.get($i)[0], regions);
//            String startRowKey = regionCode + HBaseConstant.HBASE_ROW_KEY_SPLIT
//                    + phoneNum + HBaseConstant.HBASE_ROW_KEY_SPLIT
//                    + dateSec.get($i)[0];
//            String endRowKey = regionCode + HBaseConstant.HBASE_ROW_KEY_SPLIT
//                    + phoneNum + HBaseConstant.HBASE_ROW_KEY_SPLIT
//                    + dateSec.get($i)[1];
//        }

        return dateSec;
    }

    public static void main(String[] args) throws IOException {


        HBaseQueryDao hBaseQueryDao = new HBaseQueryDao();
//        List<String[]> strings = hBaseQueryDao.genRowkeyDateSection("19775072523", "20170404", "20170701");
//        for (Iterator<String[]> iterator = strings.iterator(); iterator.hasNext(); ) {
//            String[] next = iterator.next();
//            System.out.println(next[0] + "->" + next[1]);
//        }
        hBaseQueryDao.queryRecordByPhoneNumAndDate("19775072523","20170104","20170701");

    }
}
