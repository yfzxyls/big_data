package com.soap.flink.source

import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.slf4j.LoggerFactory
import org.apache.flink.api.scala._

/**
  * @author yangfuzhao on 2018/12/28.
  */
object ReadFile {

  //  @transient lazy val logger = Logger.apply(this.getClass)
  //  log.info("hello scala log4j")

  //  val logger = Logger(this.getClass)
  val logger = Logger(LoggerFactory.getLogger("ReadFile"))

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    logger.info("aaa")
    //必须是绝对路径
    val text: DataStream[String] = env.readTextFile("/Users/soapy/IdeaProjects/big_data/flink/src/main/resources/soap.csv")

    text.flatMap(_.split(","))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print()
    logger.info("", text)

    env.execute("readFile")

    //    text.print()
    //    text.
  }

}
