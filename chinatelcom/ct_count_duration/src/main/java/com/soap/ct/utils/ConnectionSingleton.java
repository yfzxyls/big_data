package com.soap.ct.utils;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by soap on 2018/3/23.
 */
public class ConnectionSingleton {
    
    private static Connection instance = null;

    public static Connection getInstance() {
        try {
            if (instance == null || instance.isClosed() || !instance.isValid(3)) {
                synchronized (ConnectionSingleton.class) {
                    if (instance == null) {
                        instance = JDBCUtil.getConnection();
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return instance;
    }

    private ConnectionSingleton() {
    }
}
