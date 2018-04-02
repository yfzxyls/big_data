package com.soap.spark.agent

import java.text.SimpleDateFormat
import java.util.{Comparator, Date}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by soap on 2018/4/2.
  */
@SerialVersionUID(1123456L) class Click extends Serializable {
  var time: String = ""
  var province: Int = 0
  var city: Int = 0
  var user: Int = 0
  var id: Int = 0

  def this(str: String) {
    this()
    val arr = str.split(" ")

    this.time_=(arr(0))
    province = arr(1).toInt
    city = arr(2).toInt
    user = arr(3).toInt
    id = arr(4).toInt
  }

  override def toString = s"Agent($time, $province, $city, $user, $id)"
}

object Click {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("word_count").setMaster("local[*]")
    //创建SparkContext
    val sparkContext = new SparkContext(sparkConf)
    val textFile = sparkContext.textFile("D:\\study\\big_data\\spark_study\\spark_core\\agent.log")
      .flatMap(_.split("\n"))
    val clicks = textFile.map(new Click(_))
    //缓存后其他action仍然可用
    clicks.cache()
    val proAndUser = clicks.map(items => (items.province + "_" + items.id, 1))
    val proAndUserKey = proAndUser.reduceByKey(_ + _) //省_ID, 次数

    val clickCount = proAndUserKey.map { items =>
      val params = items._1.split("_")
      (params(0), (params(1), items._2))
    }
    val proClick = clickCount.groupByKey()
    val proAdClick = proClick.mapValues(
      items => items.toList.sortWith(_._2 > _._2).take(3)
    )

    println(proAdClick.collect().mkString(","))

    //需求：统计每一个省份每一个小时的TOP3广告的ID
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val proAndTimeAndId = clicks.map(items => (items.province + "_" + items.id + "_" + simpleDateFormat.format(new Date(items.time)), 1))
    val clickHour = proAndTimeAndId.reduceByKey(_ + _).map {
      items =>
        val params = items._1.split("_")
        (params(0) + "_" + params(2), (params(1), items._2))
    }.groupByKey.mapValues(items => {
      items.toList.sortWith(_._2 > _._2).take(3)
    }).map(items => {
      val param = items._1.split("_")
      (param(0), (param(1), items._2))
    }).groupByKey()
    println(clickHour.collect.mkString)
    sparkContext.stop()
  }

}
