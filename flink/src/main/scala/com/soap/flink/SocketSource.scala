package com.soap.flink

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.{FixedDelayRestartStrategyConfiguration, RestartStrategyConfiguration}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object SocketSource extends App with LazyLogging {

  /**
    * filter 返回false的数据将会被过滤
    *
    * @param args
    */
  override def main(args: Array[String]): Unit = {


    //    Logger.getRootLogger.setLevel(Level.WARN)
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("",1,"")

    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1,10000))

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", 9999, '\n')

    logger.debug("=================>>>>>>>>>>>>aaaa")

    val count = text.flatMap(_.split(" "))
      .filter(!StringUtils.isEmpty(_))
      .map((_, 1))
      .keyBy(0)
      //      .timeWindow(Time.seconds(10), Time.seconds(10))
      .sum(1).setParallelism(4)
    //.reduce((v1, v2) => (v1._1, v1._2 + v2._2))

    count.print().setParallelism(1)

    env.execute("socket")
  }
}
