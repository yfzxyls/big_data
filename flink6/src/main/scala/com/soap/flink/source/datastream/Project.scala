package com.soap.flink.source.datastream

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * @author yangfuzhao on 2019/1/21.
  */
object Project {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", 9999)


//    val count :DataStream[(String,Int,Int,Int,Int)] = text.flatMap(_.split(" ")).map((_,1,3,4,5))


    env.execute("project")
  }

}
