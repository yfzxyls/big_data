package com.soap.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Created by yangf on 2018/3/13.
 */
public class CountInterceptor implements ProducerInterceptor<String, String> {

    private int succesCount = 0;
    private int failCount = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (recordMetadata != null) {
            failCount++;
        } else {
            succesCount++;
        }
    }

    @Override
    public void close() {
        System.out.println("success : " + succesCount + ", fail : " + failCount);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
