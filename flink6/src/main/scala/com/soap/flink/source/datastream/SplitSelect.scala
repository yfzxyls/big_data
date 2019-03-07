package com.soap.flink.source.datastream

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
/**
  * @author yangfuzhao on 2019/1/17.
  * 1. split 将数据流做标记,需要使用select 选择对应标记的流
  * 2. 不支持连续split
  */
object SplitSelect extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 9999)


    val count1 = text //.flatMap(_.split(" "))
      .filter(!StringUtils.isEmpty(_))
      .map(Integer.parseInt(_))
      .split(num =>
        (num % 4) match {
          case 1 => List("1")
          case 3 => List("3")
          case _ => List("4")
        }
      ).split(num =>
      (num % 4) match {
        case 0 => List("0")
        case 2 => List("2")
        case _ => List("4")
      })

    count1.select("0").map(v=>{ println("0"); println(v)})
    //    count1.select("add").print
    count1.select("2").map(v=>{ println("2"); println(v)})
    env.execute("splitSelect")


  }

}
