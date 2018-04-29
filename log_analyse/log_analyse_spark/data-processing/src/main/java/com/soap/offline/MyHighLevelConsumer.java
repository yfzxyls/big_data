package com.soap.offline;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.stringtemplate.v4.ST;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class MyHighLevelConsumer {
    /**
     * 该consumer所属的组ID
     */
    private String groupid;

    /**
     * 该consumer的ID
     */
    private String consumerid;

    private String topic = "log_analysis";

    /**
     * 每个topic开几个线程？ 推荐设置与partition 一样，一个partition 只能被一个线程消费
     * 一个线程可以消费多个partition,如果 threadPerTopic > partition 则只有部分线程有数据
     */
    private int threadPerTopic;

    public MyHighLevelConsumer(String groupid, String consumerid, int threadPerTopic) {
        super();
        this.groupid = groupid;
        this.consumerid = consumerid;
        this.threadPerTopic = threadPerTopic;
    }

    public void consume() {
        Properties props = new Properties();
        props.put("group.id", groupid);
        props.put("consumer.id", consumerid);
        props.put("zookeeper.connect", "hadoop200:2182");
        props.put("zookeeper.session.timeout.ms", "60000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");

        ConsumerConfig config = new ConsumerConfig(props);
        kafka.javaapi.consumer.ConsumerConnector connector = Consumer.createJavaConsumerConnector(config);

        Map<String, Integer> topicCountMap = new HashMap<>();

        // 设置每个topic开几个线程
        topicCountMap.put(topic, threadPerTopic);

        // 获取stream
        Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topicCountMap);

        // 为每个stream启动一个线程消费消息
        for (KafkaStream<byte[], byte[]> stream : streams.get(topic)) {
            new MyStreamThread(stream).start();
        }
    }

    /**
     * 每个consumer的内部线程
     */
    private class MyStreamThread extends Thread {
        private KafkaStream<byte[], byte[]> stream;

        public MyStreamThread(KafkaStream<byte[], byte[]> stream) {
            super();
            this.stream = stream;
        }

        @Override
        public void run() {
            ConsumerIterator<byte[], byte[]> streamIterator = stream.iterator();

            // 逐条处理消息
            while (streamIterator.hasNext()) {
                MessageAndMetadata<byte[], byte[]> message = streamIterator.next();
                String topic = message.topic();
                int partition = message.partition();
                long offset = message.offset();
//                String key = new String(message.key());
                String msg = new String(message.message());
                // 在这里处理消息,这里仅简单的输出
                // 如果消息消费失败，可以将已上信息打印到日志中，活着发送到报警短信和邮件中，以便后续处理
                System.out.println("consumerid:" + consumerid + ", thread : " + Thread.currentThread().getName()
                        + ", topic : " + topic + ", partition : " + partition + ", offset : " + offset + " , mess : " + msg);
            }
        }
    }

    public static void main(String[] args) {
        String groupid = "log_analysis_consumer";
        MyHighLevelConsumer consumer1 = new MyHighLevelConsumer(groupid, "log_analysis_consumer", 6);
//        MyHighLevelConsumer consumer2 = new MyHighLevelConsumer(groupid, "log_analysis_consumer", 3);

        consumer1.consume();
//        consumer2.consume();
    }
}
