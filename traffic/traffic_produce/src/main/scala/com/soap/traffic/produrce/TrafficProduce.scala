package com.soap.traffic.produrce

import java.text.DecimalFormat

import com.alibaba.fastjson.JSON
import com.soap.traffic.produrce.util.PropertiesUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.joda.time.DateTime

import scala.collection.mutable
import scala.util.Random

/**
  * Created by soap on 2018/4/11.
  */
object TrafficProduce {

  def main(args: Array[String]): Unit = {
    //
    val properties = PropertiesUtil.getPeroperties()
    val produce = new KafkaProducer[String, String](properties)
    var startTime = DateTime.now().getMillis / 1000
    var flag = 1
    //每5秒变换速度
    val interval = 5
    //卡口：0001 车速：15
    //卡口：0001 车速：58
    val monitorDecimal = new DecimalFormat("0000")
    val speedDecimal = new DecimalFormat("000")
    while (true) {
      val monitorId = monitorDecimal.format(Random.nextInt(16))
      var speed = ""
      val end = DateTime.now().getMillis / 1000
      if ((end - startTime) % interval == 0) {
        startTime = end
        flag = -flag

      }
      if (flag == 1) {
        speed = speedDecimal.format(Random.nextInt(16))
      } else {
        speed = speedDecimal.format(Random.nextInt(31) + 30)
      }
      //      println("monitorId : " + monitorId + ",speed : " + speed)

      val map = new java.util.HashMap[String, String]()
      map.put("monitor_id", monitorId)
      map.put("speed", speed)
      val str = JSON.toJSON(map).toString
      println(str)
      produce.send(new ProducerRecord[String, String](PropertiesUtil.getKey("kafka.topics"), str))

      Thread.sleep(200)
    }

  }
}
