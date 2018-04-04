package com.soap.spark_sql

import org.apache.spark.sql.SparkSession

/**
  * Created by soap on 2018/4/4.
  */
object SparkSqlOutHive {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL") /*.master("spark://hadoop200:7077")*/ .master("local[*]")
      //设置spark 配置
      //.config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()
    val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://master01:3306/rdd").option("dbtable", " rddtable").option("user", "root").option("password", "hive").load()
  }
}
