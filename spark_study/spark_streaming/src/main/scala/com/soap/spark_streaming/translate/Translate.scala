package com.soap.spark_streaming.translate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Created by soap on 2018/4/6.
  */
object Translate {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val sc = new SparkConf().setAppName("streaming_translate").setMaster("local[*]")
    val ssc = new StreamingContext(sc, Seconds(5))
    val socketText = ssc.socketTextStream("hadoop200", 9999)
    //* 1.有状态转换函数必须 checkpoint 保存上一次结果
    ssc.checkpoint("./checkpoint")
    val words = socketText.flatMap(_.split(" ")).map((_, 1))
    /**
      * 使用窗口函数 统计某一时间段内的 数据
      * windowDuration 窗口大小
      * slideDuration 移动步长 两个参数必须为 ssc 扫描间隔的整数倍
      */
    val wordCount = words.reduceByKeyAndWindow((v1:Int,v2:Int)=>v1 + v2, Seconds(5),Seconds(10))
    /**
      * 每次获取数据都生成新文件
      */
//    words.saveAsTextFiles("hdfs://hadoop200:9000/spark/streaminf/output")
    /**
      * 累积操作
      * 根据时间的推进，不断的在每一个RDD上应用updateFunc函数，
      * 函数中Seq[V]表示当前RDD，当前Key所有的Value值，Option[S]表示上一次保存的状态，
      * 函数需要返回一个新的状态。整个转换操作返回一个包含每个key转换数据的DStream
      */

    //     val wordCount = words.updateStateByKey((values: Seq[Int], state: Option[Int]) =>
    //        state match {
    //          case Some(x) => Some(values.sum + x)
    //          case None => Some(values.sum)
    //        }
    //      )

    wordCount.print()
    ssc.start()
    ssc.awaitTermination()

    //    DStreamQueue(ssc)
  }

  /**
    * 从队列中读取数据
    *
    * @param ssc
    */
  private def DStreamQueue(ssc: StreamingContext) = {
    // Create the queue through which RDDs can be pushed to
    // a QueueInputDStream
    //创建RDD队列
    val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()

    // Create the QueueInputDStream and use it do some processing
    // 创建QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue)

    //处理队列中的RDD数据
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    /**
      * transform 操作 可用将 DStream 转换为RDD
      */
    //    inputStream.transform((rdd:RDD[Int])=> rdd).print()


    //打印结果
    reducedStream.print()

    //启动计算
    ssc.start()

    // Create and push some RDDs into
    for (i <- 1 to 30) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)

      //通过程序停止StreamingContext的运行
      //ssc.stop()
    }
  }
}
