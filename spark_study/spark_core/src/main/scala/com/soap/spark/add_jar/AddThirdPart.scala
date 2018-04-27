package com.soap.spark.add_jar

import org.apache.spark.{SparkConf, SparkContext}

object AddThirdPart {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("addThirdPart").setMaster("local[*]")
    val sparkSession = new SparkContext(sparkConf)
    sparkSession.addJar("")

  }

}
