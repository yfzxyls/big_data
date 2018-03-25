package com.soap.chinatelcom.consumer.utils;

import com.sun.org.apache.bcel.internal.util.ClassLoader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by yangf on 2018/3/20.
 */
public class PropertiesUtil {

    public static Properties properties = null;

    static {
        InputStream resourceAsStream = ClassLoader.getSystemResourceAsStream("hbase_kafk_consumer.properties");
        try {
            properties = new Properties();
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static String getPropertiesKey(String key){
           return properties.getProperty(key);
    }

    

}
