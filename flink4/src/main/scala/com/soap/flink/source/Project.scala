package com.soap.flink.source

import com.soap.flink.source.window.logger
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


/**
  * @author yangfuzhao on 2019/1/21. 
  */
object Project {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", 9999)


    val count :DataStream[(String,Int,Int,Int,Int)] = text.flatMap(_.split(" ")).map((_,1,3,4,5))
//    count.pr
    env.execute("project")
  }

}
