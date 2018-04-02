package com.soap.spark.spark_core

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by soap on 2018/4/2.
  */
object SparkRDD {

  def main(args: Array[String]): Unit = {

    //创建SparkConf 对象
    val sparkConf = new SparkConf().setAppName("spark_core").setMaster("local[*]")
    //.setMaster("local[*]")
    //创建SparkContext
    val sc = new SparkContext(sparkConf)
    //    val rdd = sc.parallelize(1 to 5)
    //    //分区map
    //    val res = rdd.mapPartitions((iterator: Iterator[Int]) => (Iterator(iterator.sum)))
    //    println(res.collect().mkString(","))


    /**
      * withReplacement表示是抽出的数据是否放回，true为有放回的抽样，false为无放回的抽样，
      * seed用于指定随机数生成器种子
      * fraction: 抽样比例
      */
    //    val rdd = sc.parallelize(1 to 100)
    //    val res = rdd.sample(false, 0.3, 5)
    //    res.cache()
    //    println(res.count())
    //    println(res.collect().mkString(","))

    /**
      * num : 抽取样本数
      */
    //    val res = rdd.takeSample(false, 3)
    //    println(res.length)
    //    println(res.mkString(","))

    /**
      * union  直接将两个RDD连接一起，集合向加
      */
    //    println("================union===========")
    //    val rdd = sc.parallelize(1 to 10)
    //    val rdd1 = sc.parallelize(5 to 15)
    //
    //    val res = rdd.union(rdd1)
    //    println(res.collect().mkString(","))

    //    println("================intersection===========")
    /**
      * 集合交集
      */
    //    val rdd31 = sc.parallelize(1 to 10)
    //    val rdd32 = sc.parallelize(5 to 15)
    //    val res3 = rdd31.intersection(rdd32)
    //    println(res3.collect().mkString(","))
    /**
      * 对RDD去重
      */
    //    println("================distinct===========")
    //    val rdd41 = sc.parallelize(1 to 10)
    //    val rdd42 = sc.parallelize(5 to 15)
    //    val res4 = rdd41.union(rdd42).distinct()
    //    println(res4.collect().mkString(","))
    //    println("================partitionBy===========")
    /**
      * 对RDD进行分区操作，
      * 如果原有的partionRDD和现有的partionRDD是一致的话就不进行分区，
      * 否则会生成ShuffleRDD.
      *  org.apache.spark.rdd.RDD[(Int, String)] = ShuffledRDD[4] at partitionBy at <console>:28 
      */
    //    val rdd5 = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")))
    //    val res = rdd5.partitionBy(new HashPartitioner(2))
    //    println(res.getNumPartitions)

    //    println("================reduceByKey===========")
    //    val rdd6 = sc.parallelize((List(("female", 1), ("male", 5), ("female", 5), ("male", 2))))
    //    val res6 = rdd6.reduceByKey(_ + _)
    //    println(res6.collect().mkString(","))

    //    println("================groupByKey===========")
    /**
      * (key ,CompactBuffer(v1,v2...))
      */
    //    val rdd6 = sc.parallelize((List(("female", 1), ("male", 5), ("female", 5), ("male", 2))))
    //    val res6 = rdd6.groupByKey()
    //    println(res6.collect().mkString(","))

    println("=========combineByKey============")
    val rdd7 = sc.parallelize(Array(("a", 4), ("a", 5), ("c", 5), ("d", 2), ("c", 6)))
    val res7 = rdd7.combineByKey((v) => (v, 1), (u: (Int, Int), v) => (u._1 + v, u._2 + 1),
      (c1: (Int, Int), c2: (Int, Int)) => (c1._1 + c2._1, c2._2 + c2._2))
    val arg = res7.map(item=>(item._1,item._2._1/item._2._2.toDouble))
    println(arg.collect().mkString(","))
    println(res7.collect().mkString(","))
  }

}
