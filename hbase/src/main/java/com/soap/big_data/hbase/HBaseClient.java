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
        String tableName = "ns_ct:tb_calllog";
        // System.out.println(HBaseUtil.isTableExist(conf, tableName));
        String column = "info";
        String namespace = "ns_ct";

//         System.out.println("创建表 " + HBaseUtil.createTable(conf,tableName,3,new String[]{"info"}));
        // System.out.println(HBaseUtil.isTableExist(conf, tableName));
        System.out.println("删除表：" + HBaseUtil.dropTable(conf,tableName,true));
        System.out.println("删除命名空间 ：" + HBaseUtil.dropNamespace(conf,namespace));

        //添加单行单列数据
//        System.out.println("添加数据：" + HBaseUtil.addRow(conf,"staff","1001","info","name","Mike"));
//        Map<String, Map<String, String>> staff = HBaseUtil.scanner(conf, "staff", Integer.MAX_VALUE);
//
//        for (Map.Entry<String, Map<String, String>> entry : staff.entrySet()){}
        
        //添加单行多列数据
//        System.out.println("添加数据：" + HBaseUtil.addRowToColums(conf,"multi_version","1001","info",new String[]{"name","age"},new String []{"Soap1","161"}));
//        System.out.println("添加数据：" + HBaseUtil.addRowToColums(conf,"multi_version","1001","info1",new String[]{"name","age"},new String []{"Soapa2","19"}));
//        System.out.println("添加数据：" + HBaseUtil.addRowToColums(conf,"multi_version","1001","info1",new String[]{"name","age"},new String []{"Soapa3","19"}));
//        System.out.println("添加数据：" + HBaseUtil.addRowToColums(conf,"multi_version","1001","info1",new String[]{"name","age"},new String []{"Soapa4","19"}));

//        Map<String, String> staff = HBaseUtil.getRow(conf, "staff", "1001");

//
        //一行数据
//        Map<String, Map<String, Map<String, String>>> row = new HashMap<>();
//        Map<String, Map<String, String>> colum = new HashMap<>();
//        Map<String, String> cell = new HashMap<>();
//        cell.put("name", "name");
//        cell.put("age", "age");
//        colum.put("info", cell);
//        row.put("1002", colum);
//
//        Map<String, Map<String, String>> colum1 = new HashMap<>();
//        Map<String, String> cell1 = new HashMap<>();
//        cell1.put("name3", "name3");
//        cell1.put("sex", "male");
//        colum1.put("info", cell1);
//        row.put("1003", colum1);
//
//        HBaseUtil.addRowsAndColums(conf, "staff", row);

//          Map<String,String> res = HBaseUtil.getRowMultiColumns(conf,"staff","1002","info",new String[]{"name","age"});
//          for (Map.Entry<String,String> entry : res.entrySet()){
//              System.out.println(entry.getKey() + " = " + entry.getValue());
//          }

//        System.out.println("删除数据：" +HBaseUtil.delete(conf,"staff","1003"));
//        System.out.println("删除多行数据：" + HBaseUtil.deleteMultiRow(conf,"staff",new String []{"1003","1002"}));

//        System.out.println("删除指定列1 ：" + HBaseUtil.deleteMulitRowMulitColum(conf,"staff","1001","info","name"));
//        System.out.println("删除指定列2 ：" + HBaseUtil.deleteRowMulitColums(conf,"staff","1001","info",new String[]{"name","age"}));

//        Map<String, Map<String, String>> rowInfo = new HashMap<>();
//        Map<String, String> columninfo = new HashMap<>();
//        columninfo.put("info1","name");
//        rowInfo.put("1001",columninfo);
//
//        Map<String, String> columninfo1 = new HashMap<>();
//        columninfo1.put("info","age");
//        rowInfo.put("1003",columninfo);
//        System.out.println("删除多行多列 ：" + HBaseUtil.deleteMulitRowMulitColum(conf,tableName,rowInfo));
//        Map<String,Map<String,String>> map = HBaseUtil.scanner(conf,"staff",3);
//        for (Map.Entry entry : map.entrySet()){
//            System.out.println(entry.getKey() + ":" + entry.getValue());
//        }
    }
}
