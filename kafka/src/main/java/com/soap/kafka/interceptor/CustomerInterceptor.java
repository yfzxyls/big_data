package com.soap.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * Created by yangf on 2018/3/13.
 */
public class CustomerInterceptor implements ProducerInterceptor<String,String> {
    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        return new ProducerRecord(producerRecord.topic(), producerRecord.partition(),producerRecord.timestamp(),
                System.currentTimeMillis() + "."+ producerRecord.value(), producerRecord.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
