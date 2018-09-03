package com.soap.spark.data_operation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object function extends App {
  override def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("function").setMaster("local[*]")

    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    sc.parallelize( 1 to (10 ,2),1).foreach(println)

  }
}
