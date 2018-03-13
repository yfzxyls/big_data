package com.soap.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by yangf on 2018/3/13.
 */
public class ProducerClient {

    public static void main(String[] args) {
        //配置属性
        Properties props = new Properties();

        //设置集群地址
        props.put("bootstrap.servers", "hadoop200:9092,hadoop201:9092,hadoop202:9092");
        //设置等待副本应答
        props.put("acks", "all");
        //设置最大尝试发送次数
        props.put("reties", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        List interceptors = new ArrayList();
//        interceptors.add("com.soap.kafka.interceptor.CustomerInterceptor");
//        interceptors.add("com.soap.kafka.interceptor.CountInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        KafkaProducer producer = new KafkaProducer(props);
        for (int i = 0; i < 50; i++) {
//            producer.send(new ProducerRecord("second", "key_" + i,"key" + i));
            producer.send(new ProducerRecord("first", String.valueOf(i), "key" + i), (recordMetadata, e) -> {
                if (recordMetadata != null) {
                    System.out.println("partition : " + recordMetadata.partition() + ",offset:" +
                            recordMetadata.offset());
                }
            });
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.flush();
        producer.close();
    }
}
