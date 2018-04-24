import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import kafka.api.OffsetRequest
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object AdvertStat {


  def main(args: Array[String]): Unit = {
    //
    val sparkConf = new SparkConf().setAppName("AdvertStat").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sparkConext = sparkSession.sparkContext
    val sparkStream = new StreamingContext(sparkConext, Seconds(5))
    val brokerList = ConfigurationManager.config.getString(Constants.KAFKA_BROKER)
    val topic = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)
    val kafkaParam = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "commerce-consumer-group",
      // latest : 如果有offset，使用之前的offset，如果没有，从最新的数据开始消费
      // earliest : 如果有offset，使用之前的offset，如果没有，从头开始消费
      // none: 如果有offset，使用之前的offset，如果没有，直接报错
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",//"largest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean),
      ConsumerConfig.RECEIVE_BUFFER_CONFIG -> (65536: java.lang.Integer)
    )
    //    sparkStream.receiverStream()

    val consumerDS = KafkaUtils.createDirectStream[String, String](sparkStream,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))

    //切断血缘
    val adDS = consumerDS.map(item => item.value()).repartition(10)
    adDS.cache()

    val advertStat = genAdvertStat(sparkSession, adDS)

    val provinceCityClick = genProvinceCityClick(sparkSession, adDS)

    val topProClick = topProvinceClick(sparkSession, provinceCityClick)

    genParMinute(sparkSession, adDS)

    sparkStream.start()
    sparkStream.awaitTermination()
  }

  def genParMinute(sparkSession: SparkSession,
                   adDS: DStream[String]) = {
    val timeMinute = adDS.map(item => {
      val param = item.split(" ")
      val dateTime = DateUtils.formatTimeMinute(new Date(param(0).toLong))
      (dateTime, 1L)
    }).reduceByKey(_ + _)

    timeMinute.reduceByKeyAndWindow((v1: Long, v2: Long) => (v1 + v2), Minutes(60), Minutes(1)).foreachRDD(rdd => {
      rdd.foreachPartition(minuteRRDDs => {
        val clickTrendArray = new mutable.ArrayBuffer[AdClickTrend]()

        for (item <- minuteRRDDs) {
          val keySplit = item._1.split("_")
          // yyyyMMddHHmm
          val timeMinute = keySplit(0)
          val date = timeMinute.substring(0, 8)
          val hour = timeMinute.substring(8, 10)
          val minute = timeMinute.substring(10)
          val adid = keySplit(1).toLong
          val clickCount = item._2
          clickTrendArray += AdClickTrend(date, hour, minute, adid, clickCount)
        }
        AdClickTrendDAO.updateBatch(clickTrendArray.toArray)
      })
    })
  }

  /**
    * 统计每天每省点击TOP3
    *
    * @param sparkSession
    * @param provinceCityClick
    */
  def topProvinceClick(sparkSession: SparkSession,
                       provinceCityClick: DStream[(String, Long)]) = {
    val proClickRDD = provinceCityClick.transform(clickRDD => {
      clickRDD.map { case (key, count) =>
        val keySplit = key.split("_")
        val dateKey = keySplit(0)
        val province = keySplit(1)
        val adid = keySplit(3)

        val newKey = dateKey + "_" + province + "_" + adid
        (newKey, count)
      }
      val clickCountRDD = clickRDD.reduceByKey(_ + _).map {
        case (newKey, count) => {
          val param = newKey.split("_")
          (param(0), param(1), param(2).toLong, count)
        }
      }

      import sparkSession.implicits._
      clickCountRDD.toDF("date", "pro", "adid", "count").createOrReplaceTempView("tmp_pro_click")
      //      val sql = "select date,pro,city,count, row_number() over(partition by pro order by count desc) rank from tmp_pro_click"
      val sql = "select date,pro,adid,count from (select date,pro,adid,count, row_number() over(partition by pro order by count desc) rank from tmp_pro_click)t where t.rank <=3"
      sparkSession.sql(sql).rdd
    })
    proClickRDD.foreachRDD(rdd => {
      rdd.foreachPartition { items =>
        val top3Array = new ArrayBuffer[AdProvinceTop3]()
        for (item <- items) {
          val date = item.getAs[String]("date")
          val province = item.getAs[String]("pro")
          val adid = item.getAs[Long]("adid")
          val count = item.getAs[Long]("count")
          top3Array append AdProvinceTop3(date, province, adid, count)
        }
        AdProvinceTop3DAO.updateBatch(top3Array.toArray)
      }
    })
  }


  /**
    * 统计各省市的点击量
    *
    * @param sparkSession
    * @param adDS
    */
  def genProvinceCityClick(sparkSession: SparkSession,
                           adDS: DStream[String]) = {
    //timestamp + " " + province + " " + city + " " + userid + " " + adid
    val proCityDS = adDS.map(adRDD => {
      val adParam = adRDD.split(" ")
      val date = DateUtils.formatDateKey(new Date(adParam(0).toLong))
      val pro = adParam(1)
      val city = adParam(2)
      val adid = adParam(3)
      (date + "_" + pro + "_" + city + "_" + adid, 1L)
    })

    proCityDS.reduceByKey(_ + _).foreachRDD(proCityRDD => {
      proCityRDD.foreachPartition(proCities => {
        val arrayProCity = new ArrayBuffer[AdStat]()
        for (proCity <- proCities) {
          val params = proCity._1.split("_")
          val date = params(0)
          val pro = params(1)
          val city = params(2)
          val adid = params(3).toLong
          arrayProCity.append(AdStat(date, pro, city, adid, proCity._2))
        }
        AdStatDAO.updateBatch(arrayProCity.toArray)
      })
    })
    proCityDS
  }

  /**
    * 统计某广告被某人点击次数，点击次数超过100则进黑名单不再统计
    *
    * @param sparkSession
    * @param adDS
    */
  def genAdvertStat(sparkSession: SparkSession,
                   adDS: DStream[String]) = {
    //    val blackList = sparkSession.sparkContext.broadcast(advertBlackListRdd)
//    adDS.map()
//    adDS.transform()
    val advertStat = adDS.transform(record => {
      val advertRdd = record.map(advert => {
        val advertParam = advert.split(" ")
        (advertParam(3).toLong, advert)
      })
      val advertBlackList = AdBlacklistDAO.findAll()
      //.map(item => (item.userid, true))
      val advertBlackListRdd = sparkSession.sparkContext.makeRDD(advertBlackList).map(item => (item.userid, true))

      val advertJoinRDD = advertRdd.leftOuterJoin(advertBlackListRdd)
      //黑名单过滤
      advertJoinRDD.filter {
        case (userId, (advertInfo, flag)) => {
          if (flag.isDefined && flag.get == true)
            false
          else
            true
        }
      }.map { case (userid, (log, _)) => (userid, log) }
    })

    val advertClickCount = advertStat.map {
      case (_, log) =>
        val logSplit = log.split(" ")
        val date = new Date(logSplit(0).toLong)
        // yyyyMMdd
        val dateKey = DateUtils.formatDateKey(date)
        val userId = logSplit(3)
        val adid = logSplit(4)
        val key = dateKey + "_" + userId + "_" + adid
        (key, 1L)
    }.reduceByKey(_ + _)

    advertClickCount.foreachRDD(advert => {
      advert.foreachPartition(rdds => {
        //将用户点击对某一广告的点击总数存入数据库
        val arrayAdvert = new ArrayBuffer[AdUserClickCount]()
        for (ad <- rdds) {
          val adParam = ad._1.split("_")
          val date = adParam(0)
          val userId = adParam(1).toLong
          val adid = adParam(2).toLong
          val clickCount = ad._2
          arrayAdvert.append(AdUserClickCount(date, userId, adid, clickCount))
        }
        AdUserClickCountDAO.updateBatch(arrayAdvert.toArray)
      })
    })

    //将点击次数超过100的统计出来
    val toBlackList = advertClickCount.filter {
      case (advertInfo, _) => {
        val adParam = advertInfo.split("_")
        val date = adParam(0)
        val userId = adParam(1).toLong
        val adid = adParam(2).toLong
        val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adid)
        if (clickCount > 100) true
        else false
      }
    }

    //用户黑名单统计
    toBlackList.map(item => {
      item._1.split("_")(1).toLong
    })
      .transform(userId => userId.distinct)
      .foreachRDD(userRdd => {
        userRdd.foreachPartition(items => {
          // case class AdBlacklist(userid:Long)
          val blackListArray = new ArrayBuffer[AdBlacklist]()
          for (item <- items) {
            blackListArray += AdBlacklist(item)
          }
          AdBlacklistDAO.insertBatch(blackListArray.toArray)
        })
      })
  }
}
