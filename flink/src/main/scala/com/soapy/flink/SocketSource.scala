package com.soapy.flink
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.{FixedDelayRestartStrategyConfiguration, RestartStrategyConfiguration}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.log4j.{Level, Logger}

object SocketSource extends App {

  override def main(args: Array[String]): Unit = {


    Logger.getRootLogger.setLevel(Level.WARN)
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1,10000))

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", 9999, '\n')

    text.flatMap{w => {
      print("a")
      w.split(" ")}
    }.map((_, 1)).keyBy(0)
      .timeWindow(Time.seconds(10),Time.seconds(10)).reduce((v1, v2) =>(v1._1, v1._2 + v2._2))//.print()

    env.execute("socket")
  }
}
