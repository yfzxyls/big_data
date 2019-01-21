package com.soap.flink.source

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @author yangfuzhao on 2019/1/21. 
  */
object Partition {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

   val text= env.socketTextStream("localhost",9999)

    val count1 = text.flatMap(_.split(" "))
      .filter(!StringUtils.isEmpty(_))
      .map(Integer.parseInt(_))
        .keyBy(0).partitionCustom()






    env.execute("partition")


  }

}
