package com.soap.spark.word_count

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by soap on 2018/4/1.
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    //创建SparkConf 对象
    val sparkConf = new SparkConf().setAppName("word_count").setMaster("local[*]")
    //创建SparkContext
    val sparkContext = new SparkContext(sparkConf)
    val textFile = sparkContext.textFile("hdfs://hadoop200:9000/NOTICE")
    val words = textFile.flatMap(_.split(" "))
    val wordCounts = words.map((_, 1))
    val res = wordCounts.reduceByKey(_ + _)
    res.saveAsTextFile("hdfs://hadoop200:9000/spark/word_count.txt")
//    println(res.collect.mkString(","))
    sparkContext.stop
  }
}
