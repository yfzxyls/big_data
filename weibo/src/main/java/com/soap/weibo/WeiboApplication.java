package com.soap.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Created by yangf on 2018/3/17.
 */
@SpringBootApplication
public class WeiboApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(WeiboApplication.class, args);
    }

//    @Bean
//    public HbaseTemplate getHBaseTemplate(){
//        return new HbaseTemplate();
//    }

    @Bean
    public Configuration getHBaseConf(){
        return HBaseConfiguration.create();
    }
}
