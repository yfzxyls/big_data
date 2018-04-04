package com.soap.spark_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by soap on 2018/4/4.
  */
case class Person(name:String,age:Int)
object SparkSqlNoHive {

  def main(args: Array[String]): Unit = {

    /**
      * 使用内置的hive ,不需要集成hive ,不能跑集群模式
      * 元数据需要在集群中spark work 节点均存在
      */
    //创建SparkConf()并设置App名称
    val spark = SparkSession
      .builder()
      .appName("Spark SQL") /*.master("spark://hadoop200:7077")*/ .master("local[*]")
      //设置spark 配置
      .config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()

    rdd2DataFrames(spark)

    spark.stop()
  }

  def df3ds(spark: SparkSession)={
    val peopleRdd = spark.sparkContext.textFile("spark_sql/src/main/resources/people.txt")
    import spark.implicits._
    val ds = peopleRdd.map(_.split(",")).map(
      paras => {
        Person(paras(0), paras(1).trim().toInt)
      })
      .toDS()
    ds.toDF()
  }


  /**
    * rdd -> tdDS
    * @param spark
    */
  def rdd2DataSet(spark: SparkSession) = {
    val peopleRdd = spark.sparkContext.textFile("spark_sql/src/main/resources/people.txt")
    import spark.implicits._
    val ds = peopleRdd.map(_.split(",")).map(
      paras => {
        Person(paras(0), paras(1).trim().toInt)
      })
      .toDS()
    ds.show
  
//    val rddFormDs = ds.rdd
//    println(rddFormDs.collect().mkString(","))

    //+-------+---+
//    |     _1| _2|
//    +-------+---+
//    |Michael| 29|
//    |   Andy| 30|
//    | Justin| 19|
//    +-------+---+
  }

  /**
    * rdd -> tdDF 
    * @param spark
    */
  def rdd2DataFrames(spark: SparkSession) = {
    val peopleRdd = spark.sparkContext.textFile("spark_sql/src/main/resources/people.txt")
    import spark.implicits._
    val df = peopleRdd.map(_.split(",")).map(
      paras => {
        Person(paras(0), paras(1).trim().toInt)
      })
      .toDF()
    //只能查字段 不能写sql
    val res0 = df.select("name")
    res0.show()
    df.createOrReplaceTempView("person")
    val res = spark.sql("select  name, age from person")
    res.show
  }

  def jsonLoad(spark: SparkSession) = {
    // For implicit conversions like converting RDDs to DataFrames
    //导入spark 包对象 提供spark DataFrames 特有的方法
    import spark.implicits._

    val df = spark.read.json("spark_sql/src/main/resources/people.json")

    // Displays the content of the DataFrame to stdout
    df.show()

    df.filter($"age" > 21).show()

    df.createOrReplaceTempView("persons")

    spark.sql("SELECT * FROM persons where age > 21").show()

  }


}
