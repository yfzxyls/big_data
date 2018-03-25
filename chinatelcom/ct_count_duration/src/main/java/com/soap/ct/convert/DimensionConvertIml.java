package com.soap.ct.convert;

import com.soap.ct.utils.ConnectionSingleton;
import com.soap.ct.utils.JDBCUtil;
import com.soap.ct.utils.LRUCache;
import com.soap.ct.writable.BaseDimensionWritable;
import com.soap.ct.writable.ContactDimensionWritable;
import com.soap.ct.writable.DateDimensionWritable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by soap on 2018/3/24.
 */
public class DimensionConvertIml implements IConvert {

    private ThreadLocal<Connection> conns = new ThreadLocal();

    private LRUCache lruCache = new LRUCache(10000);

    public DimensionConvertIml() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                JDBCUtil.close(conns.get(), null, null);
            }
        });
    }

    @Override
    public int getDimensionId(BaseDimensionWritable baseDimensionWritable) {
        String cacheKey = genCacheKey(baseDimensionWritable);
        Integer dimensionId = -1;
        dimensionId = lruCache.get(cacheKey);
        if (dimensionId != null && dimensionId != -1) return dimensionId;
        String[] sqls = genSql(baseDimensionWritable);
        Connection connection = getConn();
        if (baseDimensionWritable instanceof ContactDimensionWritable) {
            ContactDimensionWritable contactDimensionWritable = (ContactDimensionWritable) baseDimensionWritable;
            dimensionId = genDimensionId(connection, sqls, contactDimensionWritable);
        } else if (baseDimensionWritable instanceof DateDimensionWritable) {
            DateDimensionWritable dateDimensionWritable = (DateDimensionWritable) baseDimensionWritable;
            dimensionId = genDimensionId(connection, sqls, dateDimensionWritable);
        }
        lruCache.put(cacheKey, dimensionId);
        return dimensionId;
    }

    /**
     * 生成缓存key
     *
     * @param baseDimensionWritable
     * @return
     */
    private String genCacheKey(BaseDimensionWritable baseDimensionWritable) {
        String cacheKey = null;
        if (baseDimensionWritable instanceof ContactDimensionWritable) {
            ContactDimensionWritable contactDimensionWritable = (ContactDimensionWritable) baseDimensionWritable;
            cacheKey = contactDimensionWritable.getTelephone();
        } else if (baseDimensionWritable instanceof DateDimensionWritable) {
            DateDimensionWritable dateDimensionWritable = (DateDimensionWritable) baseDimensionWritable;
            cacheKey = dateDimensionWritable.getYear() + "_" + dateDimensionWritable.getMonth() + "_" + dateDimensionWritable.getDay();
        } else throw new IllegalArgumentException("parameter is illegal");
        return cacheKey;
    }

    /**
     * 根据不同维度获取id
     *
     * @param connection
     * @param sqls
     * @param baseDimensionWritable
     * @return
     */
    private int genDimensionId(Connection connection, String[] sqls, BaseDimensionWritable baseDimensionWritable) {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.prepareStatement(sqls[0]);
            setParameters(statement, baseDimensionWritable);
            resultSet = statement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("id");
            } else {
                statement = connection.prepareStatement(sqls[1]);
                setParameters(statement, baseDimensionWritable);
                if (statement.executeUpdate() > 0) {
                    genDimensionId(connection, sqls, baseDimensionWritable);
                } else throw new RuntimeException("insert table error!");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    /**
     * 根据传入维度数据设置 PreparedStatement 参数
     *
     * @param statement
     * @param baseDimensionWritable
     */
    private void setParameters(PreparedStatement statement, BaseDimensionWritable baseDimensionWritable) throws SQLException {
        int $i = 0;
        if (baseDimensionWritable instanceof ContactDimensionWritable) {
            ContactDimensionWritable contactDimensionWritable = (ContactDimensionWritable) baseDimensionWritable;
            statement.setString(++$i, contactDimensionWritable.getTelephone());
            statement.setString(++$i, contactDimensionWritable.getName());
        } else if (baseDimensionWritable instanceof DateDimensionWritable) {
            DateDimensionWritable dateDimensionWritable = (DateDimensionWritable) baseDimensionWritable;
            statement.setInt(++$i, Integer.valueOf(dateDimensionWritable.getYear()));
            statement.setInt(++$i, Integer.valueOf(dateDimensionWritable.getMonth()));
            statement.setInt(++$i, Integer.valueOf(dateDimensionWritable.getDay()));
        } else throw new IllegalArgumentException("parameter is illegal");

    }

    /**
     * 根据不同维度生成对应表sql
     *
     * @param baseDimensionWritable
     * @return
     */
    private String[] genSql(BaseDimensionWritable baseDimensionWritable) {
        String[] sqls = new String[2];
        if (baseDimensionWritable instanceof ContactDimensionWritable) {
            sqls[0] = "SELECT `id` FROM `tb_contacts` WHERE `telephone` = ? AND `name` = ?";
            sqls[1] = "INSERT INTO ct.tb_contacts(`telephone`,`name`) VALUES (?,?)";
        } else {
            sqls[0] = "SELECT `id` FROM `tb_dimension_date` WHERE `year` = ? AND `month` = ? AND `day` = ?";
            sqls[1] = "INSERT INTO `tb_dimension_date`(`year`, `month`, `day`) VALUES (?,?,?)";
        }
        return sqls;
    }

    public Connection getConn() {
        Connection conn = conns.get();
        try {
            if (conn == null || conn.isClosed() || !conn.isValid(3)) {
                conn = ConnectionSingleton.getInstance();
                conns.set(conn);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
}
