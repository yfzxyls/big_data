package com.soap.spark_streaming.broadcast_accumulator

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.util.LongAccumulator

/**
  * Created by soap on 2018/4/6.
  */
object WordBlacklist {

  @volatile private var instance: Broadcast[Seq[String]] = null

  def getInstance(sc: SparkContext): Broadcast[Seq[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val wordBlacklist = Seq("a", "b", "c")
          instance = sc.broadcast(wordBlacklist)
        }
      }
    }
    instance
  }
}

object DroppedWordsCounter {

  @volatile private var instance: LongAccumulator = null

  def getInstance(sc: SparkContext): LongAccumulator = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = sc.longAccumulator("WordsInBlacklistCounter")
        }
      }
    }
    instance
  }
}

object BroadcastAndAcc {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val sc = new SparkConf().setAppName("streaming_translate").setMaster("spark://hadoop200:7077")
    val ssc = new StreamingContext(sc, Seconds(5))
    val socketText = ssc.socketTextStream("hadoop200", 9999)
    val wordCounts = socketText.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    wordCounts.foreachRDD {
      (rdd: RDD[(String, Int)], time: Time) =>
        // Get or register the blacklist Broadcast
        val blacklist = WordBlacklist.getInstance(rdd.sparkContext)
        // Get or register the droppedWordsCounter Accumulator
        val droppedWordsCounter = DroppedWordsCounter.getInstance(rdd.sparkContext)
        // Use blacklist to drop words and use droppedWordsCounter to count them
        val counts = rdd.filter {
          case (word, count) =>
            if (blacklist.value.contains(word)) {
              droppedWordsCounter.add(count)
              false
            } else {
              true
            }
        }//.collect().mkString("[", ", ", "]")
        val output = "Counts at time " + time + "," + droppedWordsCounter.count
        println(output)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}