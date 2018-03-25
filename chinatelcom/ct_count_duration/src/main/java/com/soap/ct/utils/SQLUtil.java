package com.soap.ct.utils;

import org.apache.hadoop.hbase.util.CollectionUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by soap on 2018/3/23.
 */
public class SQLUtil {
    //
//    String sql = "INSERT INTO ct.tb_contacts(telephone,`name`) VALUES (?,?)" +
//            " ON DUPLICATE KEY UPDATE b=VALUES(b),c=VALUES(c);";

    /**
     * 插入电话记录表
     *
     * @param dateId
     * @param contactId
     * @param sum
     * @param duration
     * @param conn
     * @return
     */
    public static boolean insertCall(String dateId, String contactId, String sum, String duration, Connection conn) {
        String insertSql = "INSERT INTO ct.tb_call(id_date_contact, id_date_dimension, id_contact, call_sum, call_duration_sum) VALUES (?,?,?,?,?)" +
                " ON DUPLICATE KEY UPDATE call_sum = ?,call_duration_sum = ?;";
        try {
            return insertOrUpdate(insertSql, new String[]{contactId + "_" + dateId, dateId, contactId, sum, duration,sum, duration}, conn) > 0;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }


    /**
     * 插入日期信息 如果日期存在则返回id 如果不存在则添加数据并返回id 如果id不存在则返回-1
     *
     * @param year
     * @param month
     * @param day
     * @param conn
     * @return
     */
    public static int insertDateDimension(String year, String month, String day, Connection conn) {
        String querySql = "select id from ct.tb_dimension_date where year = ? and month = ? and day = ?";
        ArrayList<Map<String, String>> result = null;
        try {
            result = query(querySql, new String[]{year, month, day}, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (CollectionUtils.isEmpty(result)) {
            String sql = "INSERT INTO ct.tb_dimension_date(year,month,day) VALUES (?,?,?)";
            try {
                insertOrUpdate(sql, new String[]{year, month, day}, conn);
                result = query(querySql, new String[]{year, month, day}, conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (result.get(0) != null) return Integer.valueOf(result.get(0).get("id"));
        return -1;
    }

    /**
     * 插入联系人信息 如果电话已存在则返回id 如果不存在则添加信息并返回id
     *
     * @param telPhone
     * @param name
     * @param conn
     * @return
     */
    public static int insertContact(String telPhone, String name, Connection conn) {
        String querySql = "select id from ct.tb_contacts where telephone = ?";
        ArrayList<Map<String, String>> result = null;
        try {
            result = query(querySql, new String[]{telPhone}, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (CollectionUtils.isEmpty(result)) {
            String sql = "INSERT INTO ct.tb_contacts(telephone,`name`) VALUES (?,?)";
            try {
                insertOrUpdate(sql, new String[]{telPhone, name}, conn);
                result = query(querySql, new String[]{telPhone}, conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (result.get(0) != null) return Integer.valueOf(result.get(0).get("id"));
        return -1;
    }

    /**
     * 可以用来处理增、删、改的方法(连接不关闭)
     *
     * @throws SQLException
     * @sql 相关sql语句
     * @values 相关的值(sql语句中?部分的值)
     * @con 数据库连接
     */
    public static int insertOrUpdate(String sql, String values[], Connection conn) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(sql);
        if (values != null) {
            for (int i = 0; i < values.length; i++) {
                stmt.setObject(i + 1, values[i]);
            }
        }
        int flag = stmt.executeUpdate();
        stmt.close();
        return flag;
    }


    /**
     * 可以用来处理增、删、改的方法(连接不关闭)
     *
     * @throws SQLException
     * @sql 相关sql语句
     * @values 相关的值(sql语句中?部分的值)
     * @con 数据库连接
     */
    public static int insertOrUpdateOrDelete(String sql, String values[], Connection conn) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(sql);
        if (values != null) {
            for (int i = 0; i < values.length; i++) {
                stmt.setObject(i + 1, values[i]);
            }
        }
        ResultSet resultSet = stmt.executeQuery();
        int id = -1;
        while (resultSet.next()) {
            id = resultSet.getInt(1);
        }
        stmt.close();
        return id;
    }


    /**
     * 根据给出的SQL进行查询，并将查询结果放到MAP里面，将拼成List返回(连接不关闭)
     *
     * @return 查询结果
     * @throws SQLException
     * @sql 相关sql语句
     * @values 相关的值(sql语句中?部分的值)
     * @con 数据库连接
     */
    public static ArrayList<Map<String, String>> query(String sql, String[] values, Connection con) throws SQLException {
        ArrayList<Map<String, String>> result = new ArrayList<>();
        PreparedStatement stmt = con.prepareStatement(sql);
        if (values != null) {
            for (int i = 0; i < values.length; i++) {
                stmt.setObject(i + 1, values[i]);
            }
        }
        ResultSet rs = stmt.executeQuery();
        ResultSetMetaData rmd = stmt.getMetaData();
        int columnCount = rmd.getColumnCount();
        while (rs.next()) {
            Map<String, String> map = new HashMap<>();
            for (int i = 0; i < columnCount; i++) {
                String columnName = rmd.getColumnName(i + 1);
                map.put(columnName, rs.getString(columnName));
            }
            result.add(map);
        }
        rs.close();
        stmt.close();
        return result;
    }


//    public static void main(String[] args) {
//        Connection connection = ConnectionSingleton.getInstance();
////        System.out.println(SQLUtil.insertContact("1234","张扬",connection));  22
////        System.out.println(SQLUtil.insertDateDimension("2017", "01", "01", connection)); //263
//        System.out.println(SQLUtil.insertCall("263", "22", "1","154", connection)); //263
//
//    }

}
