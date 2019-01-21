package com.soap.flink.source

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


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

///**
//  * state中保存的数据类型
//  */
//case class CountWithTimestamp(key: String, count: Long, lastModified: Long)
//
///**
//  * ProcessFunction的实现，用来维护计数和超时
//  */
//class CountWithTimeoutFunction extends ProcessFunction[(String, String), (String, Long)] {
//
//  /** process function维持的状态  */
//  lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext
//    .getState(new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp]))
//
//
//  override def processElement(value: (String, String), ctx: Context, out: Collector[(String, Long)]): Unit = {
//    // 初始化或者获取/更新状态
//
//    val current: CountWithTimestamp = state.value match {
//      case null =>
//        CountWithTimestamp(value._1, 1, ctx.timestamp)
//      case CountWithTimestamp(key, count, lastModified) =>
//        CountWithTimestamp(key, count + 1, ctx.timestamp)
//    }
//
//    // 将状态写回
//    state.update(current)
//
//    // 从当前事件时间开始计划下一个60秒的定时器
//    ctx.timerService.registerEventTimeTimer(current.lastModified + 60000)
//  }
//
//  override def onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[(String, Long)]): Unit = {
//    state.value match {
//      case CountWithTimestamp(key, count, lastModified) if (timestamp == lastModified + 60000) =>
//        out.collect((key, count))
//      case _ =>
//    }
//  }
//}

