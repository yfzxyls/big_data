package com.soap.chinatelcom.consumer.kafka_consumer;

import com.soap.chinatelcom.consumer.dao.HBaseSaveDao;
import com.soap.chinatelcom.consumer.utils.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;

/**
 * Created by yangf on 2018/3/20.
 */
public class HBaseKafkaClient {

    public static void main(String[] args) {
        KafkaConsumer kafkaConsumer = new KafkaConsumer(PropertiesUtil.properties);
        kafkaConsumer.subscribe(Arrays.asList(PropertiesUtil.getPropertiesKey("hbase_kafka_topice")));

        HBaseSaveDao hBaseDao = new HBaseSaveDao();
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
                if (!StringUtils.isBlank(record.value()))
                    hBaseDao.saveData(record.value());
            }
        }
    }


}
