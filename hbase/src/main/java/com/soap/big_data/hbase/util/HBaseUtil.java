package com.soap.big_data.hbase.util;


import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

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

    public static boolean createNameSpace(Configuration conf, String namseSpace) {
        Admin admin = null;
        try {
            admin = ConnectionFactory.createConnection(conf).getAdmin();
            NamespaceDescriptor ns = NamespaceDescriptor.create(namseSpace).
                    addConfiguration("creater", "soap").build();
            admin.createNamespace(ns);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    /**
     * @param conf
     * @param tableName    表名不能重复  表存在将抛出异常
     * @param columnFamily 列族必须有
     * @return
     */
    public static boolean createTable(Configuration conf, String tableName, int maxVersion, String... columnFamily) throws Exception {
        try {
            if (columnFamily == null || columnFamily.length <= 0) {
                return false;
            }
            Admin admin = ConnectionFactory.createConnection(conf).getAdmin();
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
                for (int i = 0; i < columnFamily.length; i++) {
                    hTableDescriptor.addFamily(new HColumnDescriptor(columnFamily[i]).setMaxVersions(maxVersion));
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
     *
     * @param conf
     * @param tableName
     * @param forceDel  是否强制删除表
     * @return
     */
    public static boolean dropTable(Configuration conf, String tableName, boolean forceDel) throws Exception {
        Admin admin = null;
        try {
            admin = ConnectionFactory.createConnection(conf).getAdmin();
            if (forceDel) {
                admin.disableTable(TableName.valueOf(tableName));
            }
            admin.deleteTable(TableName.valueOf(tableName));

        } catch (TableNotDisabledException e) {
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            admin.close();
        }

        return true;
    }

    /**
     * 添加一行数据
     *
     * @param conf
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param columnName
     * @param vaule
     * @return
     */
    public static boolean addRow(Configuration conf, String tableName, String rowkey, String columnFamily, String columnName, String vaule) {
        Table table = null;
        try {
            table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(vaule));
            table.put(put);
            table.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 添加一行多列数据
     *
     * @param conf
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnNames  多列数据的key
     * @param vaules       多列数据的value 必须与列对应
     * @return
     */
    public static boolean addRowToColums(Configuration conf, String tableName, String rowKey, String columnFamily, String[] columnNames, String[] vaules) {
        try {
            Table table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            if (columnNames.length != vaules.length) {
                throw new RuntimeException("columnNames length must eq values length");
            }
            for (int i = 0; i < columnNames.length; i++) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnNames[i]), Bytes.toBytes(vaules[i]));
            }
            table.put(put);
            table.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean addByPutList(Configuration conf, String tableName, List<Put> puts) {
        if (CollectionUtils.isEmpty(puts)) return true;
        Table table = null;
        try {
            table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
            table.put(puts);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
        return false;
    }

    public static void closeTable(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 添加多个rowKey 的多列数据
     *
     * @param conf
     * @param tableName
     * @param rowInfo
     * @return
     */
    public static boolean addRowsAndColums(Configuration conf, String tableName, Map<String, Map<String, Map<String, String>>> rowInfo) {
        Table table = null;
        try {
            table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
            List<Put> puts = new ArrayList<>();
            generaterMuilt(rowInfo, puts);
            table.put(puts);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    private static void generaterMuilt(Map<String, Map<String, Map<String, String>>> rowInfo, List<Put> puts) {
        for (Map.Entry<String, Map<String, Map<String, String>>> outEntry : rowInfo.entrySet()) {
            Put put = new Put(Bytes.toBytes(outEntry.getKey()));
            for (Map.Entry<String, Map<String, String>> innerEntry : outEntry.getValue().entrySet()) {
                byte[] columFaily = Bytes.toBytes(innerEntry.getKey());
                for (Map.Entry<String, String> entry : innerEntry.getValue().entrySet()) {
                    put.addColumn(columFaily, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                }
            }
            puts.add(put);
        }
    }


    /**
     * 根据 rowKey 删除整条数据
     *
     * @param conf
     * @param tableName
     * @param rowKey
     * @return
     */
    public static boolean deleteRow(Configuration conf, String tableName, String rowKey) {
        return deleteMultiRow(conf, tableName, rowKey, null, null);
    }


    /**
     * 删除多行
     *
     * @param conf
     * @param tableName
     * @param rowKey
     * @return
     */
    public static boolean deleteMultiRow(Configuration conf, String tableName, String... rowKey) {
        Table table = null;
        try {
            table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));

            List<Delete> deletes = new ArrayList<>();
            for (int i = 0; i < rowKey.length; i++) {
                Delete delete = new Delete(Bytes.toBytes(rowKey[i]));
                deletes.add(delete);
            }
            table.delete(deletes);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    /**
     * 删除rowKey 的指定多列
     *
     * @param conf
     * @param tableName
     * @return
     */
    public static boolean deleteMulitRowMulitColum(Configuration conf, String tableName, Map<String, Map<String, String>> rowInfo) {
        Table table = null;
        try {
            table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
            List<Delete> deletes = new ArrayList<>();
            for (Map.Entry<String, Map<String, String>> outEntry : rowInfo.entrySet()) {
                Delete delete = new Delete(Bytes.toBytes(outEntry.getKey()));
                for (Map.Entry<String, String> innerEntry : outEntry.getValue().entrySet()) {
                    byte[] columnFamily = Bytes.toBytes(innerEntry.getKey());
                    delete.addColumn(columnFamily, Bytes.toBytes(innerEntry.getValue()));
                }
                deletes.add(delete);
            }
            table.delete(deletes);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }


    public static boolean deleteRowMulitColums(Configuration conf, String tableName, String rowKey, String columFaily, String... colum) {
        Table table = null;
        try {
            table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            if (!StringUtils.isBlank(columFaily) && colum != null && colum.length > 0) {
                for (int i = 0; i < colum.length; i++) {
                    delete.addColumn(Bytes.toBytes(columFaily), Bytes.toBytes(colum[i]));
                }
            }
            table.delete(delete);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }


    /**
     * 根据delete集合删除
     *
     * @param conf
     * @param tableName
     */
    public static void deleteByDeletes(Configuration conf, String tableName, List<Delete> deleteList) {
        Table table = null;
        try {
            table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
            table.delete(deleteList);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
    }

    /**
     * 根据rowKey 查询所有列信息 返回Map
     *
     * @param conf
     * @param tableName
     * @param rowKey
     * @return
     */
    public static Map<String, String> getRow(Configuration conf, String tableName, String rowKey) {
        return getRowMultiColumns(conf, tableName, rowKey, null);
    }


    public static Map<String, String> getRowMultiColumns(Configuration conf, String tableName, String rowKey, String columnFamily, String... column) {
        Table table = null;
        try {
            table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            if ((column != null && column.length > 0) && !StringUtils.isBlank(columnFamily)) {
                for (int i = 0; i < column.length; i++) {
                    get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column[i]));
                }
            } else if (!StringUtils.isBlank(columnFamily)) {
                get.addColumn(Bytes.toBytes(columnFamily), null);
            }
            Result result = table.get(get);
            String row = Bytes.toString(result.getRow());
            if (StringUtils.isBlank(row)) {
                return new HashMap<>();
            }
            Map<String, String> cellMap = new HashMap<>();
            for (Cell cell : result.rawCells()) {
                cellMap.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }
            return cellMap;
        } catch (IOException e) {
            e.printStackTrace();

        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 根据scan集合查找rowkey
     *
     * @param conf
     * @param tableName
     * @param scans
     */
    public static List<String> getRowkeysByScans(Configuration conf, String tableName, List<Scan> scans, int maxVersion) {
        if (CollectionUtils.isEmpty(scans)) return null;
        Table table = null;
        Set<String> rowkeys = new HashSet<>();
        try {
            table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));

            for (Iterator<Scan> iterator = scans.iterator(); iterator.hasNext(); ) {
                Scan next = iterator.next();
                if (maxVersion > 0) {
                    next.setMaxVersions(maxVersion);
                }
                ResultScanner resultScanner = table.getScanner(next);
                for (Result result : resultScanner) {
                    String rowkey = Bytes.toString(result.getRow());
                    rowkeys.add(rowkey);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
        return new ArrayList<>(rowkeys);
    }


    public static Map<String, Map<String, String>> getRowByGets(Configuration conf, String tableName, List<Get> gets) {
        Map<String, Map<String, String>> resultMap = null;
        Table table = null;
        try {
            table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
            Result[] results = table.get(gets);
            resultMap = new HashMap<>();
            for (Result result : results) {
                String rowKey = Bytes.toString(result.getRow());
                Map<String, String> cellMap = new HashMap<>();
                for (Cell cell : result.rawCells()) {
                    cellMap.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
                resultMap.put(rowKey, cellMap);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
        return resultMap;

    }

    public static Map<String, Map<String, String>> scannerByColumn(Configuration conf, String tableName, String columnFamily, int maxVersion, String... columns) {
        Table table = null;
        try {
            table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            if (maxVersion >= 0) {
                scan.setMaxVersions(maxVersion);
            }
            if (!StringUtils.isBlank(columnFamily)) {
                for (int i = 0; i < columns.length; i++) {
                    scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i]));
                }
            }
            ResultScanner resultScanner = table.getScanner(scan);
            return generateResultMap(resultScanner);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
        }
        return null;
    }

    private static Map<String, Map<String, String>> generateResultMap(ResultScanner resultScanner) {
        Map<String, Map<String, String>> rows = new HashMap<>();
        for (Result result : resultScanner) {
            String row = Bytes.toString(result.getRow());
            Map<String, String> cellMap = new HashMap<>();
            for (Cell cell : result.rawCells()) {
                cellMap.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
            }
            rows.put(row, cellMap);
        }
        return rows;
    }

    public static Map<String, Map<String, String>> scanner(Configuration conf, String tableName, int maxVersion) {
        return scannerByColumn(conf, tableName, null, maxVersion, null);
    }
}
