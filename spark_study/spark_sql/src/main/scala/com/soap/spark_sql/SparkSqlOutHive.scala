package com.soap.spark_sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by soap on 2018/4/4.
  */
object SparkSqlOutHive {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL ")
      .master("spark://hadoop200:7077")
      .enableHiveSupport()//.master("local[*]")
      //设置spark 配置
      //.config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()
      spark.read.parquet()


//    val jdbcDF: DataFrame = loadFormJDBC(spark)
//    jdbcDF.show()
    spark.stop()
  }

  /**
    * 从数据库读取数据
    * @param spark
    * @return
    */
  private def loadFormJDBC(spark: SparkSession) = {
    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop200:3306/company")
      .option("dbtable", " staff")
      .option("user", "hadoop").option("password", "hadoop").load()
    jdbcDF
  }
}
