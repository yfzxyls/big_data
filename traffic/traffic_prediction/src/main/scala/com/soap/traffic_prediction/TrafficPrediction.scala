package com.soap.traffic_prediction

import java.time.format.DateTimeFormatterBuilder
import java.util.Date

import com.soap.traffic_prediction.utils.RedisPool
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeUtils}

import scala.collection.mutable.ArrayBuffer

object TrafficPrediction {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("TrafficPrediction").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //预估时间 214414
    val inputDateStr = "2018-04-13 21:44:14"
    val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    //时间解析
    val inputDate = DateTime.parse(inputDateStr, format)

    //从redis 中取出 指定时间的相关数据
    val redisConn = RedisPool.apply("hadoop200").pool.getResource
    redisConn.select(1)

    val monitorIDs = List("0005", "0015")
    val monitorRelations = Map[String, Array[String]](
      "0005" -> Array("0003", "0004", "0005", "0006", "0007"),
      "0015" -> Array("0013", "0014", "0015", "0016", "0017"))
    monitorIDs.map(monitorId => {
      //取相关数据
      val monitorRelationInfo = monitorRelations(monitorId).map(relationId => {
        (relationId, redisConn.hgetAll(inputDate.toString("yyyyMMdd") + "_" + relationId))
      })

      val dataX = new ArrayBuffer[Double]()

      for (i <- 0 to 2) {
        val startHm = inputDate.getMillis - 60 * i * 10000
        val startHmStr = new DateTime(startHm).toString("HHmm")
        for ((k, v) <- monitorRelationInfo) {
          if (v.containsKey(startHmStr)) {
            val sumAndCount = v.get(startHmStr).split("_")
            dataX += sumAndCount(0).toDouble / sumAndCount(1).toDouble
          } else {
            dataX += 60F
          }
        }
      }
      //将数据组装为 Vectors
      val dataVector = Vectors.dense(dataX.toArray)
      //使用 LogisticRegressionModel 加载模型进行预估
      val pathStr = monitorId + "_" + inputDate.toString("yyyyMMddHHmm")
      val hdfsPath = redisConn.hget("model",pathStr)

      val model = LogisticRegressionModel.load(sc, hdfsPath)
      val predict = model.predict(dataVector)
      println(monitorId + "_" + "堵车评估值：" + predict + "通畅级别：" + (if (predict > 3) "通畅" else "拥堵"))

      //保存结果
      redisConn.hset(inputDateStr, monitorId, predict.toString)


    })
  }
}
