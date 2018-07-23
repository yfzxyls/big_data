package com.soap.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object TestString {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("rdd_zip").setMaster("local[*]")

    val sc = new SparkContext(conf)
//    val rdd1 = sc.makeRDD(Seq((1,1), (2,2), (3,3), (4,4), (5,5), (6,6)), 3)
    val rdd1 = sc.makeRDD(Seq(11, 22, 33, 44, 55, 66), 3)
    val rdd2 = sc.makeRDD(Seq(1, 2, 3, 4, 5, 6), 3)
    Logger.getRootLogger.setLevel(Level.WARN)
//
//    val a = rdd1.distinct().zipWithIndex().collectAsMap()
//    val b = rdd1.map(x => {
//      a.get(x).get
//    })


    //println(b.collect().mkString(","))
    //    val rdd = rdd1.zip(rdd2)
    //val zip = rdd1.zipWithUniqueId
    //val zip1 = rdd1.zipWithIndex
    //    rdd.collect()
    // println(zip.collect().mkString(","))
    // println(zip1.collect().mkString(","))
    //    val a = ((31 -1) / 10 +1 ) * 10 *20
    //    print(a)
    //
    //    val list1 = List(1, 3, 1, 5, 5, 6)
    //    val list2 = List(1, 5, 3, 4, 5)
    //
    //    val v = list1.zip(list2).zip(list2)

    // println(v)
    //    val v2 = v.unzip
    //    println(v2)
    //1,2,3,4,5,6


    //val arr = 10 to 20 by 3
    //val arry = Range(10.0,20.0,3.0).toArray
    //println(arr.mkString(","))
    //println(arry.mkString(","))


    val zip2 = rdd1.zip(rdd2)//.map(x=>{(x._1.productIterator.toBuffer.+=(x._2))})
    //val zip3 = zip2.zip(rdd2).map(x=>{})
    println(zip2.collect.mkString("|"))
    //zip3.foreach(println)
  }
}
