package com.soap.spark_sql.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SaveTable {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(getClass.getSimpleName)
      .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //    saveToHiveTable(sparkSession)
    val person = List(("Nick", 22, 1), ("Make", 21, 1), ("Make", 24, 1),
      ("Thoms", 23, 2), ("Jick", 45, 2), ("Jick1", 23, 2),
      ("a", 12, 3), ("b", 23, 3), ("c", 11, 3))
    import sparkSession.implicits._
    val personDF = sparkSession.sparkContext.makeRDD(person).toDF("name","age","class")
    personDF.createOrReplaceTempView("person")
//    val sql = "select * from person"
    //row_number 生成行号
//    val sql = "select * ,row_number() OVER (PARTITION BY class ORDER BY age DESC) row from person "
    //排序开窗函数必须有 ORDER BY
//    val sql = "select * ,row_number() OVER (PARTITION BY class ) row from person "
    //取各班级平均值大于20
//    val sql = "select * from (select * ,avg(age) OVER (PARTITION BY class) age_avg from person) where  age_avg > 20"
    //
    val sql = "select * ,rank(age) OVER (PARTITION BY class ORDER BY age DESC) rank from person"
    sparkSession.sql(sql) show


  }

  /**
    * 保存数据至hive 表
    *
    * @param sparkSession
    */
  private def saveToHiveTable(sparkSession: SparkSession) = {
    val person = List(("Nick", 22), ("Make", 21), ("Make", 24), ("Thoms", 23))

    import sparkSession.implicits._
    val personDF = sparkSession.sparkContext.makeRDD(person).toDF("name", "age")
    //mode 默认为 ErrorIfExists
    personDF.write
      .mode(SaveMode.Append)
      // .bucketBy(2,"name")
      .sortBy("name")
      .saveAsTable("person_sort")
  }
}
