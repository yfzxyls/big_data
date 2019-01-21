package com.soap.flink.source

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

/**
  * @author yangfuzhao on 2019/1/15. 
  */
object UnionJoin extends LazyLogging {

  /**
    * union 将两个流合并为一个流,同一个流union 数据翻倍
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("",1,"")

    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1,10000))

    // get input data by connecting to the socket
    val text1 = env.socketTextStream("localhost", 9999)

    logger.debug("=================>>>>>>>>>>>>aaaa")
    val text2 = env.socketTextStream("localhost", 9998)

    //    val text = text1.union(text1)

    //    val text = text1.join(text2).where(0).equalTo(1)

    val count1 = text1.map(v => {
      val user = v.split(" ")
      User(user(0), user(1))
    }).keyBy(0)
    //      .keyBy(0)
    //      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
    //      .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
    //      .window(TumblingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(4)))
    //      .window(GlobalWindows.create()).trigger(trigger)
    //        .maxBy(0).print()
    //      .sum(1)

    val count2 = text2.map(v => {
      val user = v.split(" ")
      User(user(0), user(1))
    }).keyBy(0)


    val count = count1.join(count2).where(_.name).equalTo(_.age).window(TumblingProcessingTimeWindows.of(Time.minutes(1)))

    count.apply((u1, u2) => {
      print(u1)
      print(u2)
      List(new User(u1.name, u2.age), new User(u2.name, u1.age))
    }).print()
    //    count.print().setParallelism(1)

    env.execute("union_join")
  }
}

case class User(name: String, age: String)

