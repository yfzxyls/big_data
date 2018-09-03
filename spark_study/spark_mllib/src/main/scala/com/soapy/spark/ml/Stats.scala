package com.soapy.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

object Stats {

  def main(args: Array[String]): Unit = {



    val conf = new SparkConf().setMaster("local[*]").setAppName("stats")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    val data = sc.textFile("/Users/soapy/IdeaProjects/big_data/spark_study/spark_mllib/src/main/data/sample_stat.txt")
      .map(_.split("\t")).map(x => x.map(_.toDouble))

    val vect = data.map(f => Vectors.dense(f))

    //按照列进行计算
    val colStat = Statistics.colStats(vect)

    println(colStat.max)
    println(colStat.min)
    //平均值
    println(colStat.mean)
    //方差
    println(colStat.variance)

    println(colStat.count)

    //统计不为0 的个数
    println(colStat.numNonzeros)

    println(colStat.normL1)
    println(colStat.normL2)



  }

}
