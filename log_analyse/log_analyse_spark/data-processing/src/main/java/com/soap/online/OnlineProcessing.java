package com.soap.online;

import com.soap.key.UserCityPackageKey;
import com.soap.model.StartupReportLogs;
import com.soap.utils.JSONUtil;
import com.soap.utils.PropertiesUtil;
import com.soap.utils.RedisUtil;
import com.soap.utils.StringUtil;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import redis.clients.jedis.Jedis;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class OnlineProcessing {

    public static void main(String[] args) throws Exception {
        //获取配置信息
        Properties properties = PropertiesUtil.getProperties("D:\\study\\big_data\\log_analyse\\log_analyse_spark\\data-processing\\src\\main\\resources\\config.properties");

        //获取检查点路径
        String checkpoint = properties.getProperty("streaming.checkpoint.path");

        //创建sparkConf
        SparkConf sparkConf = new SparkConf().setAppName("OnlineProcessing").setMaster("local[*]");
        // 配置spark streaming 优雅停机
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
        // 配置每秒从kafka的一个分区获取多少条数据
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100");
        // 配置spark的序列化类 KryoSerializer
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        // 指定Kryo注册器
        sparkConf.set("spark.kryo.registrator", "com.soap.registrator.MyKryoRegistrator");

        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpoint, creatingFunc(sparkConf, properties));

        //启动 spark streaming
        jsc.start();
        jsc.awaitTermination();
    }

    /**
     * 如果不存在检查点则创建 spark streaming 并执行计算操作，并将计算流程 checkpoint
     *
     * @return
     */
    private static Function0<JavaStreamingContext> creatingFunc(final SparkConf sparkConf, final Properties properties) {

        //准备kafka消费者参数

        final Map<String, String> kafkaParam = new HashMap<>();
        kafkaParam.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("kafka.broker.list"));
        kafkaParam.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("kafka.group.id")); //
        final String topic = properties.getProperty("kafka.topic");
        final String groupId = properties.getProperty("kafka.group.id");

        Function0<JavaStreamingContext> function0 = new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {

                //获取 kafka Cluster
                final KafkaCluster kafkaCluster = getKafkaCluster(kafkaParam);

                Set topicSet = new HashSet<>(Arrays.asList(topic.split(",")));

                //获取偏移量信息
                final Map<TopicAndPartition, Long> partitionOffsetMap = getConsumerOffsets(kafkaCluster, groupId, topicSet);

                //获取streaming 对象
                JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

                //手动设置 checkpoint
                javaStreamingContext.checkpoint(properties.getProperty("streaming.checkpoint.path"));

                JavaInputDStream<String> inputDStream = KafkaUtils.createDirectStream(javaStreamingContext,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        String.class,
                        kafkaParam,
                        partitionOffsetMap, new Function<MessageAndMetadata<String, String>, String>() {
                            //只获取 value
                            @Override
                            public String call(MessageAndMetadata<String, String> v1) throws Exception {
                                return v1.message();
                            }
                        });


                //打印测试
//                inputDStream.foreachRDD(
//                        new VoidFunction<JavaRDD<String>>() {
//                            @Override
//                            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
//                                stringJavaRDD.foreach(new VoidFunction<String>() {
//                                    @Override
//                                    public void call(String s) throws Exception {
//                                        System.out.println(s);
//                                    }
//                                });
//                            }
//                        }
//                );

                //获取offset信息


                final AtomicReference<OffsetRange[]> offsetRangeAtomicReference = new AtomicReference<>();
                inputDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
                    @Override
                    public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                        OffsetRange[] offsetRange = ((HasOffsetRanges) stringJavaRDD.rdd()).offsetRanges();
                        offsetRangeAtomicReference.set(offsetRange);
                    }
                });

                //过滤操作
                JavaPairDStream<UserCityPackageKey, Long> filerPairCountDS = inputDStream.filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String v1) throws Exception {

                        //过滤日志类型
                        if (!v1.contains("appVersion") && !v1.contains("currentPage") &&
                                !v1.contains("errorMajor")) {
                            return false;
                        }

                        // 暂时只获取appVersion 日志
                        if (!v1.contains("appVersion")) {
                            return false;
                        }

                        //过滤关键信息
                        StartupReportLogs startupReportLogs = null;
                        startupReportLogs = JSONUtil.json2Object(v1, StartupReportLogs.class);
                        if (startupReportLogs == null ||
                                StringUtil.isEmpty(startupReportLogs.getAppVersion()) ||
                                StringUtil.isEmpty(startupReportLogs.getUserId()) ||
                                StringUtil.isEmpty(startupReportLogs.getCity())) {
                            return false;
                        }
                        return true;
                    }
                }).mapToPair(new PairFunction<String, UserCityPackageKey, Long>() {
                    @Override
                    public Tuple2<UserCityPackageKey, Long> call(String s) throws Exception {
                        StartupReportLogs startupReportLogs = JSONUtil.json2Object(s, StartupReportLogs.class);
                        UserCityPackageKey userCity = new UserCityPackageKey();
                        userCity.setCity(startupReportLogs.getCity());
                        userCity.setUserId(Long.valueOf(startupReportLogs.getUserId().substring(4)));
                        return new Tuple2<>(userCity, 1L);
                    }
                }).reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });


                filerPairCountDS.foreachRDD(new VoidFunction<JavaPairRDD<UserCityPackageKey, Long>>() {
                    @Override
                    public void call(JavaPairRDD<UserCityPackageKey, Long> pairRDD) throws Exception {
                        pairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<UserCityPackageKey, Long>>>() {
                            @Override
                            public void call(Iterator<Tuple2<UserCityPackageKey, Long>> tuple2Iterator) throws Exception {
                                Jedis jedis = RedisUtil.getJedis();
                                while (tuple2Iterator.hasNext()) {
                                    Tuple2<UserCityPackageKey, Long> tuple2 = tuple2Iterator.next();
                                    String city = tuple2._1.getCity();
                                    Long user = tuple2._1.getUserId();
                                    Long count = tuple2._2;

                                    //如果用户已存在则不存
                                    if(StringUtil.isEmpty(jedis.hget(city,String.valueOf(user)))){
                                        jedis.hset(city, String.valueOf(user), String.valueOf(count));
                                    }

                                }
                                RedisUtil.returnResource(jedis);
                            }
                        });
                        offsetToZk(kafkaCluster,offsetRangeAtomicReference,groupId);
                    }
                });
                return javaStreamingContext;
            }
        };


        return function0;
    }


    /**
     * 将offset写入zk
     */
    public static void offsetToZk(final KafkaCluster kafkaCluster,
                                  final AtomicReference<OffsetRange[]> offsetRanges,
                                  final String groupId) {
        // 遍历每一个偏移量信息
        for (OffsetRange o : offsetRanges.get()) {

            // 提取offsetRange中的topic和partition信息封装成TopicAndPartition
            TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
            // 创建Map结构保持TopicAndPartition和对应的offset数据
            Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap();
            // 将当前offsetRange的topicAndPartition信息和untilOffset信息写入Map
            topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());

            // 将Java的Map结构转换为Scala的mutable.Map结构
            scala.collection.mutable.Map<TopicAndPartition, Object> testMap = JavaConversions.mapAsScalaMap(topicAndPartitionObjectMap);

            // 将Scala的mutable.Map转化为imutable.Map
            scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap =
                    testMap.toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
                        public Tuple2<TopicAndPartition, Object> apply(Tuple2<TopicAndPartition, Object> v1) {
                            return v1;
                        }
                    });

            // 更新offset到kafkaCluster
            kafkaCluster.setConsumerOffsets(groupId, scalatopicAndPartitionObjectMap);
        }
    }

    /**
     * 获取kafka每个分区消费到的offset,以便继续消费
     */
    public static Map<TopicAndPartition, Long> getConsumerOffsets(KafkaCluster kafkaCluster, String groupId, Set<String> topicSet) {
        // 将Java的Set结构转换为Scala的mutable.Set结构
        scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicSet);
        // 将Scala的mutable.Set结构转换为immutable.Set结构
        scala.collection.immutable.Set<String> immutableTopics = mutableTopics.toSet();
        // 根据传入的分区，获取TopicAndPartition形式的返回数据
        scala.collection.immutable.Set<TopicAndPartition> topicAndPartitionSet2 = (scala.collection.immutable.Set<TopicAndPartition>)
                kafkaCluster.getPartitions(immutableTopics).right().get();


        // 创建用于存储offset数据的HashMap
        // (TopicAndPartition, Offset)
        Map<TopicAndPartition, Long> consumerOffsetsLong = new HashMap();

        // kafkaCluster.getConsumerOffsets：通过kafkaCluster的getConsumerOffsets方法获取指定消费者组合，指定主题分区的offset
        // 如果返回Left，代表获取失败，Zookeeper中不存在对应的offset，因此HashMap中对应的offset应该设置为0
        if (kafkaCluster.getConsumerOffsets(groupId, topicAndPartitionSet2).isLeft()) {

            // 将Scala的Set结构转换为Java的Set结构
            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            // 由于没有保存offset（该group首次消费时）, 各个partition offset 默认为0
            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                consumerOffsetsLong.put(topicAndPartition, 0L);
            }
        } else {
            // offset已存在, 获取Zookeeper上的offset
            // 获取到的结构为Scala的Map结构
            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp =
                    (scala.collection.immutable.Map<TopicAndPartition, Object>)
                            kafkaCluster.getConsumerOffsets(groupId, topicAndPartitionSet2).right().get();

            // 将Scala的Map结构转换为Java的Map结构
            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);

            // 将Scala的Set结构转换为Java的Set结构
            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            // 将offset加入到consumerOffsetsLong的对应项
            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                Long offset = (Long) consumerOffsets.get(topicAndPartition);
                consumerOffsetsLong.put(topicAndPartition, offset);
            }
        }

        return consumerOffsetsLong;
    }

    /**
     * 获取kafka 集群信息
     *
     * @param kafkaParams
     * @return
     */
    public static KafkaCluster getKafkaCluster(Map<String, String> kafkaParams) {
        // 将Java的HashMap转化为Scala的mutable.Map
        scala.collection.mutable.Map<String, String> testMap = JavaConversions.mapAsScalaMap(kafkaParams);
        // 将Scala的mutable.Map转化为imutable.Map
        scala.collection.immutable.Map<String, String> scalaKafkaParam =
                testMap.toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
                    public Tuple2<String, String> apply(Tuple2<String, String> v1) {
                        return v1;
                    }
                });

        // 由于KafkaCluster的创建需要传入Scala.HashMap类型的参数，因此要进行上述的转换
        // 将immutable.Map类型的Kafka参数传入构造器，创建KafkaCluster
        return new KafkaCluster(scalaKafkaParam);
    }
}
