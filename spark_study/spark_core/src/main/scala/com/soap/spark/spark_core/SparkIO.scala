package com.soap.spark.spark_core

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.ShortTypeHints
import org.json4s.jackson.Serialization

/**
  * Created by soap on 2018/4/4.
  */

object SparkIO {


  /**
    * 1.读取操作支持模糊匹配
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("io").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //    jsonFile(sc)
    //    seqFile(sc)
    //    hadoopFile(sc)
//    mysqlIO(sc)
    val data = sc.parallelize(List("Female", "Male", "Female"))
    data.foreachPartition(insertData)
    sc.stop
  }

  /**
    * 数据插入mysql
    * @param iterator
    */
  def insertData(iterator: Iterator[String]): Unit = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://hadoop200:3306/company", "hadoop", "hadoop")
    iterator.foreach(data => {
      val ps = conn.prepareStatement("insert into staff(name) values (?)")
      ps.setString(1, data)
      ps.executeUpdate()
    })
  }

  def mysqlIO(sc: SparkContext) = {
    val rdd = new org.apache.spark.rdd.JdbcRDD(
      sc,
      () => {
        Class.forName("com.mysql.jdbc.Driver").newInstance()
        java.sql.DriverManager.getConnection("jdbc:mysql://hadoop200:3306/ct", "hadoop", "hadoop")
      },
      "select * from tb_contacts where id >= ? and id <= ?;",
      1,
      10,
      1,
      r => (r.getString(1), r.getString(2)))
    rdd.cache()
    println(rdd.count())
    rdd.foreach(println(_))
  }

  /**
    *  1.FileOutputFormat 必须使用实现类 否则反射无法创建对象  每个分区一个文件
    * MapFileOutputFormat 每个分区输出index 和data 两个文件
    *
    * @param sc
    */
  def hadoopFile(sc: SparkContext) = {
    val data = sc.parallelize(Array((30, "hadoop"), (71, "hive"), (11, "cat")))
    data.saveAsNewAPIHadoopFile("hdfs://hadoop200:9000/spark/output_map",
      classOf[IntWritable], classOf[Text], classOf[org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat])
  }

  /**
    * 1.只能读取序列化的键值对
    *
    * @param sc
    */
  def seqFile(sc: SparkContext) = {
    //    val data=sc.parallelize(List((2,"aa"),(3,"bb"),(4,"cc"),(5,"dd"),(6,"ee")))
    //    data.saveAsSequenceFile("D:\\study\\big_data\\spark_study\\spark_core\\src\\main\\resource\\people1")
    val seq = sc.sequenceFile[Int, String]("D:\\study\\big_data\\spark_study\\spark_core\\src\\main\\resource\\people1\\part-0000*")
    seq.map(x => new Person(x._2, x._1)).collect().foreach(println(_))

  }

  /**
    * json 文件输入输出
    *
    * @param sc
    */
  def jsonFile(sc: SparkContext) = {
    val jsonFile = sc.textFile("D:\\study\\big_data\\spark_study\\spark_core\\src\\main\\resource\\employees.json")
    /**
      * parse  需要导入一下包
      * import org.json4s._
      * import org.json4s.jackson.JsonMethods._
      * 2.如果需要后续使用json rdd 需要将隐式转换放在parse 前
      */
    val persons = jsonFile.map(x => {
      implicit val formats = Serialization.formats(ShortTypeHints(List()))
      parse(x).extract[Person]
    })
    persons.collect().foreach(x => println(x.name + "," + x.salary))
    /**
      * json  输出 ，重写toString 方法
      */
    persons.saveAsTextFile("D:\\study\\big_data\\spark_study\\spark_core\\src\\main\\resource\\employees1")

    //    val result = jsonFile.flatMap(record => {
    //      try {
    //        Some(mapper.readValue(record, classOf[Person]))
    //      } catch {
    //        case e: Exception => None
    //      }
    //    })
    //    result.collect().foreach(
    //      x => println(x.name)
    //    )
  }
}
