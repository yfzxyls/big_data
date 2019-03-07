package com.soap.flink.source.datastream

import java.util.Date
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

/**
  * @author yangfuzhao on 2019/1/15.
  *         GlobalWindows 全局窗口慎用，可指定 trigger 触发窗口关闭
  *         SessionWindow 指定窗口期没有数据将会触发窗口关闭
  *         TumblingWindow 滚动窗口：窗口内数据不重复
  *         SlidingWindow 滑动窗口：窗口内数据可能重复
  *
  *
  *         窗口的开启和关闭以整秒、整分或整时，如果需要以其他时间开启或关闭，需要设置偏移量
  *
  */
object window extends LazyLogging {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("",1,"")

    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1,10000))

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", 9999)

    logger.debug("=================>>>>>>>>>>>>aaaa")

    val count = text.flatMap(_.split(" "))
      .filter(!StringUtils.isEmpty(_))
      .map((_, 1))
      .keyBy(0)
      .window(ProcessingTimeSessionWindows.withGap(Time.minutes(1)))
      //      .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
      //      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      //      .window(GlobalWindows.create()).trigger(trigger)
      //        .maxBy(0).print()
      .reduce((v1, v2) => {
      println(new Date + "===========>>>>>>" + Thread.currentThread.getName)
      println(v1 + "," + v2)
      (v1._1, v1._2 + v2._2)
    })

    /**
      * 运行在主线程
      */
    println("=>>>>>>>>>>>" + new Date + Thread.currentThread.getName)

    count.print().setParallelism(1)

    env.execute("socket")
  }


}
