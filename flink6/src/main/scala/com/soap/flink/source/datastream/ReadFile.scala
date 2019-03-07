package com.soap.flink.source.datastream


import com.typesafe.scalalogging.Logger
import org.apache.flink.api.java.io.TextValueInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.{FileMonitoringFunction, FileProcessingMode}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.StringValue
import org.slf4j.LoggerFactory

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
    val path = s"/Users/soapy/IdeaProjects/big_data/flink/src/main/resources/text/kv.csv"

    val text = readByFileStream(env, path)
    //    val text: DataStream[String] = env.readTextFile("/Users/soapy/IdeaProjects/big_data/flink/src/main/resources/soap.csv")

//    val count = text.print()
    //      .flatMap {
    //      _.split(",") filter (StringUtils.isNotBlank(_))
    //    }
    //      .filter(StringUtils.isNotBlank(_))
    //      .map((_, 1))
    //      .keyBy(0) //keyBy DataStream的API groupBy DataSet的API
    //      .sum(1)
    //      .setParallelism(2)

//        text.flatMap(_.split(",")).map((_, 1))
//          .keyBy(0)
//          .reduce((v1, v2) => (v1._1, v1._2 + v2._2)).setParallelism(2)
    //      .print().setParallelism(1)
    //
    val keyStream = text.map(v=>{
      val s = v.split(",")
      (Integer.valueOf(s(0)),Integer.valueOf(s(1)))
     })//.setParallelism(8)
      .keyBy(0).timeWindow(Time.seconds(2))//.window(TumblingEventTimeWindows.of(Time.seconds(5)))


    /**
      * 慎用
      */
    //    keyStream.min(0).print()

    /**
      * 返回第几个字段最新的元素
      */
    keyStream.minBy(1).print()
    keyStream.maxBy(1).print()
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

    env.execute("readFile")

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
  def readText(env: StreamExecutionEnvironment, path: String): DataStream[String] = {
    env.readTextFile(path)
  }


  /**
    * PROCESS_ONCE 读取一次
    * PROCESS_CONTINUOUSLY 定期读取全量文件，不是增量
    *
    * @param env
    * @param path
    * @return
    */
  def readByFile(env: StreamExecutionEnvironment, path: String): DataStream[StringValue] = {
    env.readFile(new TextValueInputFormat(new Path), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 1000L)
  }


  /**
    * ONLY_NEW_FILES, // Only new files will be processed. default
    * REPROCESS_WITH_APPENDED, 重新获取有变化的整个文件,新文件也只获取新文件内容 //When some files are appended, all contents of the files will be processed.
    * PROCESS_ONLY_APPENDED 仅重跑新增内容 // When some files are appended, only appended contents will be processed.
    * 起多个子线程，一个线程监控整个文件夹的改变，当检测的改变后，将内容切分然后分派给对应的子线程处理
    *
    * @param env
    * @param path
    */
  def readByFileStream(env: StreamExecutionEnvironment, path: String): DataStream[String] = {
    env.readFileStream(path, 500L, FileMonitoringFunction.WatchType.REPROCESS_WITH_APPENDED)
  }


}
