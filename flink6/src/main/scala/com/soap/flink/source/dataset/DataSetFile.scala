package com.soap.flink.source.dataset

import com.typesafe.scalalogging.Logger
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.slf4j.LoggerFactory

/**
  * @author yangfuzhao on 2018/12/28.
  */
object DataSetFile {

  //  @transient lazy val logger = Logger.apply(this.getClass)
  //  log.info("hello scala log4j")

  //  val logger = Logger(this.getClass)
  val logger = Logger(LoggerFactory.getLogger("ReadFile"))

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    logger.info("aaa")

    //必须是绝对路径
    val path = s"/Users/soapy/IdeaProjects/big_data/flink6/src/main/resources/text"

    val text = readByFileDs(env, path)

    val keyStream = text.map(v=>{
      val s = v.split(",")
      (Integer.valueOf(s(0)),Integer.valueOf(s(1)))
     })//.setParallelism(8).print()

//      .keyBy(0).timeWindow(Time.seconds(2))//.window(TumblingEventTimeWindows.of(Time.seconds(5)))

    //      .fold(0)((v1, v2) => (v1 + v2._2)).setParallelism(2)
    //      .print().setParallelism(1)


    /**
      * NO_OVERWRITE 自动创建文件夹，文件存在则报错
      * OVERWRITE :覆盖
      */
    //    count.writeAsCsv("/Users/soapy/IdeaProjects/big_data/flink/src/main/resources/count.csv", FileSystem.WriteMode.OVERWRITE, "\n", ",").setParallelism(1)
    //
    //    count.print()
    logger.info("", text)

//    env.execute("readFileDs")
    keyStream.print()
    //    text.print()
    //    text.
  }


  /**
    * 将指定文件作为输入流
    * readTextFile
    * 1.使用TextInputFormat 读取文件，按行读取
    *
    * @param env
    * @param path
    * @return
    */
//  def readText(env: ExecutionEnvironment, path: String): DataStream[String] = {
//    env.readTextFile(path)
//  }


//  /**
//    * PROCESS_ONCE 读取一次
//    * PROCESS_CONTINUOUSLY 定期读取全量文件，不是增量
//    *
//    * @param env
//    * @param path
//    * @return
//    */
//  def readByFile(env: StreamExecutionEnvironment, path: String): DataStream[StringValue] = {
//    env.readFile(new TextValueInputFormat(new Path), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000L)
//  }


  /**
    * @param env
    * @param path
    */
  def readByFileDs(env: ExecutionEnvironment, path: String): DataSet[String] = {
    env.readTextFile(path)
  }


}
