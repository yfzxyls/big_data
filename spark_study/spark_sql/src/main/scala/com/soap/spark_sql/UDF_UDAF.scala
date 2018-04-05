package com.soap.spark_sql

import org.apache.spark.sql.SparkSession

/**
  * Created by soap on 2018/4/4.
  */
object UDF_UDAF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("udf") /*.master("spark://hadoop200:7077")*/ .master("local[*]")
      .getOrCreate()



    UDAFStrogr(spark)
//    UDAFWeak(spark)
//    udf(spark)
    spark.stop()
  }

  private def UDAFStrogr(spark: SparkSession) = {
    import spark.implicits._
    val ds = spark.read.json("spark_sql/src/main/resources/employees.json").as[Employee]
    val averageSalary = MyAverage.toColumn.name("average_salary")
    val result = ds.select(averageSalary)
    result.show()
  }

  /**
    * 自定义弱类型DAF
    *
    * @param spark
    */
  private def UDAFWeak(spark: SparkSession) = {
    val df = spark.read.json("spark_sql/src/main/resources/employees.json")
    df.createOrReplaceTempView("employees")
    spark.udf.register("myAverage", MyAverageWeak)
    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
  }

  private def udf(spark: SparkSession) = {
    val peopleRdd = spark.read.json("spark_sql/src/main/resources/people.json")
    spark.udf.register("person_name", (x: String) => "name:" + x)
    peopleRdd.createOrReplaceTempView("person")
    spark.sql("select person_name(name),age from person").show()
  }

  
}
