package com.soap.spark_sql.stock

import org.apache.spark.sql.SparkSession

/**
  * Created by soap on 2018/4/4.
  */
@SerialVersionUID(1l) case class tbStock(ordernumber: String, locationid: String, dateid: String) extends Serializable

@SerialVersionUID(1l) case class tbStockDetail(ordernumber: String, rownum: Int, itemid: String, number: Int, price: Double, amount: Double) extends Serializable

@SerialVersionUID(1l) case class tbDate(dateid: String, years: Int, theyear: Int, month: Int, day: Int, weekday: Int, week: Int, quarter: Int, period: Int, halfmonth: Int) extends Serializable

object Stock {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Stock") /*.master("spark://hadoop200:7077") */ .master("local[*]")
      //设置spark 配置
      // .config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()

    init(spark)
//    countYearStock(spark)
//    countYearAmountMax(spark)
    countYearItemMax(spark)
    spark.stop()
  }

  /**
    * 加载数据
    *
    * @param spark
    */

  def init(spark: SparkSession): Unit = {
    import spark.implicits._
    val tbStockRdd = spark.sparkContext.textFile("spark_sql/src/main/resources/tbStock.txt")
    val tbStockDS = tbStockRdd.map(_.split(",")).map(attr => tbStock(attr(0), attr(1), attr(2))).toDS
    val tbStockDetailRdd = spark.sparkContext.textFile("spark_sql/src/main/resources/tbStockDetail.txt")
    val tbStockDetailDS = tbStockDetailRdd.map(_.split(",")).map(attr => tbStockDetail(attr(0), attr(1).trim().toInt, attr(2), attr(3).trim().toInt, attr(4).trim().toDouble, attr(5).trim().toDouble)).toDS
    val tbDateRdd = spark.sparkContext.textFile("spark_sql/src/main/resources/tbDate.txt")
    val tbDateDS = tbDateRdd.map(_.split(",")).map(attr => tbDate(attr(0), attr(1).trim().toInt, attr(2).trim().toInt, attr(3).trim().toInt, attr(4).trim().toInt, attr(5).trim().toInt, attr(6).trim().toInt, attr(7).trim().toInt, attr(8).trim().toInt, attr(9).trim().toInt)).toDS
    //    tbDateDS.show()
    //    tbStockDS.show()
    //    tbStockDetailDS.show()
    tbStockDS.createOrReplaceTempView("tbStock")
    tbDateDS.createOrReplaceTempView("tbDate")
    tbStockDetailDS.createOrReplaceTempView("tbStockDetail")
  }

  //统计所有订单中每年的销售单数、销售总额 订单有重复
  def countYearStock(spark: SparkSession) = {
    val countYearStock = spark.sql("SELECT d.theyear,count(DISTINCT sd.ordernumber) ,sum(sd.amount) FROM tbStock s   JOIN tbDate d ON  d.dateid = s.dateid   JOIN  tbStockDetail sd ON sd.ordernumber = s.ordernumber  GROUP BY d.theyear  ORDER BY d.theyear")
    countYearStock.show()
  }

  //  9.4计算所有订单每年最大金额订单的销售额
  //    目标：统计每年最大金额订单的销售额:
  def countYearAmountMax(spark: SparkSession) = {
    val countYearMax = spark.sql("SELECT d.theyear, MAX(max) FROM ( SELECT sd.ordernumber, s.dateid, SUM(sd.amount) AS max FROM tbStock s JOIN tbStockDetail sd ON sd.ordernumber = s.ordernumber GROUP BY sd.ordernumber, s.dateid ) t2 JOIN tbDate d ON t2.dateid = d.dateid GROUP BY d.theyear")
    countYearMax.show()
  }

  //9.5计算所有订单中每年最畅销货品
//  目标：统计每年最畅销货品（哪个货品销售额amount在当年最高，哪个就是最畅销货品）
  def countYearItemMax (spark:SparkSession)={
    val countYearItemMax = spark.sql("SELECT t2.itemid,td1.theyear from \n(SELECT d.theyear, itemid,sum(sd.amount) as sum \nfrom tbStockDetail sd \nJOIN tbStock s ON sd.ordernumber = s.ordernumber\njoin tbDate d  on  d.dateid = s.dateid \nGROUP BY sd.itemid,d.theyear) \nt2 JOIN tbStockDetail sd1 on t2.itemid = sd1.itemid \nJOIN tbDate td1 on td1.theyear = t2.theyear GROUP BY  t2.itemid,td1.theyear")
    countYearItemMax.show()
  }

}