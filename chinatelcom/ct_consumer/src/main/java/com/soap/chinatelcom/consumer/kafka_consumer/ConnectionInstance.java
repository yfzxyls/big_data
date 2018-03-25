package com.soap.chinatelcom.consumer.kafka_consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by yangf on 2018/3/21.
 */
public class ConnectionInstance {

    private static Connection connection;

    private ConnectionInstance() {

    }
    private static  Configuration conf = HBaseConfiguration.create();
    public static synchronized Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }
}
