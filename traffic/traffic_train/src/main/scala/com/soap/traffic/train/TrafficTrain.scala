package com.soap.traffic.train

import java.io.{File, PrintWriter}

import com.soap.traffic.train.utils.RedisPool
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.joda.time.DateTime

import scala.collection.immutable.Range
import scala.collection.mutable.ArrayBuffer

object TrafficTrain {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("TrafficTrain").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)

    val writer = new PrintWriter(new File("spark/train.log"))

    //模拟相关数据,关卡本身也是相关数据
    val monitorIds = List("0005", "0015")
    val relationData = Map[String, Array[String]](
      "0005" -> Array("0003", "0004", "0005", "0006", "0007"),
      "0015" -> Array("0013", "0014", "0015", "0016", "0017"))

    val redisConn = RedisPool.apply("hadoop200").pool.getResource
    redisConn.select(1)
    val date = DateTime.now()
    monitorIds.map(monitorId => {

      //(0003,(0413,644_14))
      //(0004,(0413,644_14))
      //(0005,(0413,644_14))
      //(0006,(0413,644_14))
      //(0007,(0413,644_14))
      val monitorRelationList = relationData.get(monitorId).get

      val relationInfo = monitorRelationList.map(relationId => {
        val key = date.toString("yyyyMMdd") + "_" + relationId
        val values = redisConn.hgetAll(key)
        (relationId, values)
      })

      val trainData = new ArrayBuffer[LabeledPoint]()
      val dataX = new ArrayBuffer[Double]()
      val dataY = new ArrayBuffer[Double]()

      val hours = 1
      //分析一小时内的数据，用前三分钟预测第四分钟
      //每分钟循环
      for (i <- Range(60 * hours, 2, -1)) {
        //每次循环清除上一次数据
        dataX.clear()
        dataY.clear()
        //样本起始时间： 当前时间往前一小时
        val sectionStart = date.getMillis - 60 * 1000 * i
        //取每一段时间第四分钟作为结果集
        for (section <- 0 to 2) {
          //样本时间 1512
          val sectionHM = sectionStart + section * 1000 * 60
          val sectionHMStr = new DateTime(sectionHM).toString("HHmm")
          for ((k, v) <- relationInfo) {
            if (k == monitorId && section == 2) {
              val nextHm = sectionHM + section * 1000 * 60
              val nextHmStr = new DateTime(nextHm).toString("HHmm")
              if (v.containsKey(nextHmStr)) {
                //获取结果
                val res = v.get(nextHmStr).split("_")
                val valueY = res(0).toFloat / res(1).toFloat
                dataY += valueY
              }
            }
            if (v.containsKey(sectionHMStr)) {
              val res = v.get(sectionHMStr).split("_")
              val valueX = res(0).toFloat / res(1).toFloat
              dataX += valueX
            } else {
              //实际在某一时刻没有车通过，则表示畅通，使用最高时速为默认值
              dataX += 60F
            }
          }
        }
        //得到一个样本
        if (dataY.length == 1) {
          val labelValue = dataY.head
          var label = 0
          if (labelValue / 10 < 10) {
            label = labelValue.toInt / 10
          } else label = 10
          val labeledPoint = new LabeledPoint(label, Vectors.dense(dataX.toArray))
          trainData += labeledPoint
        }
      }
      trainData.foreach(e => {
        writer.write(e.toString() + "\n\t")
        writer.flush()
      })

      //将得到的所有数据分为训练集和测试集
      val trainRdd = sc.parallelize(trainData)
      val randomSplits = trainRdd.randomSplit(Array(0.6, 0.4), 11L)
      val train = randomSplits(0)
      val test = randomSplits(1)

      //准备模型
      val model = new LogisticRegressionWithLBFGS().setNumClasses(11).run(train)
      //对模型进行评估
      val predictAndLabel = test.map {
        case LabeledPoint(label, features) => {
          val predict = model.predict(features)
          (predict, label)
        }
      }
      //得到当前评估值
      val metrics = new MulticlassMetrics(predictAndLabel)
      val accuracy = metrics.accuracy
//      println(accuracy)
      //将评估结果保存到本地文件中
      writer.write("accuracy: " + accuracy.toString + "\r\n")
      //保存模型
      if (accuracy >= 0.0) {
        val date = DateTime.now().toString("yyyyMMddHHmm")
        val hdfsPath = "hdfs://hadoop200:9000/traffic/model/" +
          monitorId +
          "_" + date
        model.save(sc, hdfsPath)
        //key:model
        //fields:0005
        //value:hdfsPath
        redisConn.hset("model", monitorId + "_" + date, hdfsPath)
      }
    })
    writer.flush()
    writer.close()
    redisConn.close()
  }
}
