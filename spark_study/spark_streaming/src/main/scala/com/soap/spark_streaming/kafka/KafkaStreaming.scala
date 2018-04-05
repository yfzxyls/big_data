package com.soap.spark_streaming.kafka

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * Created by soap on 2018/4/5.
  */
object KafkaStreaming {

  val logger: Logger = Logger.getLogger("KafkaStreaming")

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //获取配置参数
    val brokenList = "hadoop200:9092,hadoop201:9092,hadoop202:9092"
    val zookeeper = "hadoop200:2181,hadoop201:2181,hadoop202:2181"
    val sourceTopic = "source"
    val targetTopic = "target"
    val groupid = "kafka_stream_consumer"

    //创建Kafka的连接参数
    val kafkaParam = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokenList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    loadFromKafkaByZookeeperOffset(ssc, zookeeper, sourceTopic, kafkaParam)
    //    loadFromKafka(ssc, sourceTopic, kafkaParam)

    ssc.start()
    ssc.awaitTermination()
  }

  private def loadFromKafkaByZookeeperOffset(ssc: StreamingContext, zookeeper: String, sourceTopic: String, kafkaParam: Map[String, String]) = {
    val brokenList = kafkaParam.getOrElse[String](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "")
    val groupid = kafkaParam.getOrElse[String](ConsumerConfig.GROUP_ID_CONFIG, "")
    var textKafkaDStream: InputDStream[(String, String)] = null

    //获取ZK中保存group + topic 的路径
    val topicDirs = new ZKGroupTopicDirs(kafkaParam.getOrElse[String](ConsumerConfig.GROUP_ID_CONFIG, ""), sourceTopic)
    //最终保存消费group id Offset topic 的地方   /consumers/kafka_streaming/offsets/source
    val zkTopicOffsetPath = s"${topicDirs.consumerOffsetDir}"

    val zkClient = new ZkClient(zookeeper)
    //保存 各分区的 Offset [0, 1, 2]
    val childrens = zkClient.countChildren(zkTopicOffsetPath)

    // 判ZK中是否有保存的数据
    if (childrens > 0) {
      //从ZK中获取Offset，根据Offset来创建连接
      var fromOffsets: Map[TopicAndPartition, Long] = Map()

      //首先获取每一个分区的主节点
      val topicList = List(sourceTopic)
      //向Kafka集群获取所有的元信息， 你随便连接任何一个节点都可以
      val request = new TopicMetadataRequest(topicList, 0)

      //kafka集群中任意节点ip port
      val hostAndPort = brokenList.split(",")(0).split(":")
      //ip port  soTimeout  bufferSize   clientId:客服端名称
      val leaderConsumer = new SimpleConsumer(hostAndPort(0), hostAndPort(1).toInt, 100000, 10000, "OffsetLookup")
      //该请求包含所有的元信息，主要拿到 分区 -》 主节点
      val response = leaderConsumer.send(request)
      val topicMetadataOption = response.topicsMetadata.headOption
      //从分区信息中获取 分区 id 和 leader 所在主机 ip
      val partitions = topicMetadataOption match {
        case Some(tm) => tm.partitionsMetadata.map(
          pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]
        case None => Map[Int, String]()
      }
      leaderConsumer.close()

      logger.warn("partitions information is: " + partitions)
      logger.warn("children information is: " + childrens)

      // childrens [0,1,2] 与分区数一致
      for (i <- 0 until childrens) {
        //先从ZK读取i这个分区的offset保存
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")

        logger.warn(s"partition[${i}] 目前的offset是：${partitionOffset}")

        // 从当前i的分区主节点去读最小的offset，
        val tp = TopicAndPartition(sourceTopic, i)
        //                              requestInfo: Map[TopicAndPartition, PartitionOffsetRequestInfo]
        val requestMinOffset = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
        val consumerMinOffset = new SimpleConsumer(partitions(i), hostAndPort(1).toInt, 10000, 10000, "GetMiniOffset")

        val curOffsets = consumerMinOffset.getOffsetsBefore(requestMinOffset).partitionErrorAndOffsets(tp).offsets
        consumerMinOffset.close()

        //合并这两个offset
        var nextOffset = partitionOffset.toLong
        if (curOffsets.length > 0 && nextOffset < curOffsets.head) {
          nextOffset = curOffsets.head
        }

        logger.warn(s"Partition[${i}] 修正后的偏移量是：${nextOffset}")
        fromOffsets += (tp -> nextOffset)
      }

      zkClient.close()
      logger.warn("从ZK中恢复创建Kafka连接")
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      textKafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParam, fromOffsets, messageHandler)
    } else {
      //直接创建到Kafka的连接
      logger.warn("直接创建Kafka连接")
      textKafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, Set(sourceTopic))
    }

    var offsetRanges = Array[OffsetRange]()
    //注意，要想获得offsetRanges必须作为第一步

    val textKafkaDStream2 = textKafkaDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    send2Kafka(brokenList, textKafkaDStream2,groupid,sourceTopic,zookeeper,offsetRanges)


  }

  private def send2Kafka(brokenList: String, textKafkaDStream2: DStream[(String, String)],
                         groupid: String, sourceTopic: String ,
                        zookeeper: String ,offsetRanges:Array[OffsetRange] ) ={
    textKafkaDStream2.map(s => "key:" + s._1 + " value:" + s._2).foreachRDD { rdd =>
      //RDD操作
      rdd.foreachPartition {
        items =>
          //需要用到连接池技术
          //创建到Kafka的连接
          val pool = KafkaConnPool(brokenList)
          //拿到连接
          val kafkaProxy = pool.borrowObject()
          //插入数据
          for (item <- items)
            kafkaProxy.send("", item)

          //返回连接
          pool.returnObject(kafkaProxy)
      }
      //保存Offset到ZK
      val updateTopicDirs = new ZKGroupTopicDirs(groupid, sourceTopic)
      val updateZkClient = new ZkClient(zookeeper)
      for (offset <- offsetRanges) {
        //将更新写入到Path
        logger.warn(offset)
        val zkPath = s"${updateTopicDirs.consumerOffsetDir}/${offset.partition}"
        ZkUtils.updatePersistentPath(updateZkClient, zkPath, offset.fromOffset.toString)
      }
      updateZkClient.close()
    }
  }

  /**
    * 直接从 kafka brokenList 中读取数据  group 不从zookeeper 中查找偏移量
    *
    * @param ssc
    * @param sourceTopic
    * @param kafkaParam
    */
  private def loadFromKafka(ssc: StreamingContext, sourceTopic: String, kafkaParam: Map[String, String]) = {
    val textKafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, Set(sourceTopic))
    textKafkaDStream.flatMap(_._2.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _).print(100)
  }
}
