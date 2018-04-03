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

    /**
      * 集合操作
      */
    //    println("==============交集intersection==================")
    //    val rdd31 = sc.parallelize(1 to 10)
    //    val rdd32 = sc.parallelize(5 to 15)
    //    val res3 = rdd31.intersection(rdd32)
    //    println(res3.collect().mkString(","))
    //    println("==============差集==================")
    //    val rdd33 = sc.parallelize(1 to 10)
    //    val rdd34 = sc.parallelize(5 to 15)
    //    val res4 = rdd33.subtract(rdd34)
    //    println(res4.collect().mkString(","))
    /**
      * 1.只合并key相同的
      * 2.value 类型不限制
      * 3. 合并后value 为元组
      */
    //    println("==================join============")
    //    val rdd41 = sc.parallelize(Array((5, "aaa"), (7, "bbb"), (3, "ccc"), (4, "41")))
    //    val rdd42 = sc.parallelize(Array((1, "aaa"), (2, 22), (3, "ccc"), (4, 42)))
    //    val res4 = rdd41.join(rdd42)
    //    println(res4.collect().mkString(","))

    /**
      * 1.只合并左边集合中所有k ,右集合中存在则添加Some(),没有则添加None
      * (4,(41,Some(42))),(5,(aaa,None)),(7,(bbb,None)),(3,(ccc,Some(ccc)))
      */
    //        println("==================leftOuterJoin============")
    //        val rdd41 = sc.parallelize(Array((5, "aaa"), (7, "bbb"), (3, "ccc"), (4, "41")))
    //        val rdd42 = sc.parallelize(Array((1, "aaa"), (2, 22), (3, "ccc"), (4, 42)))
    //        val res4 = rdd41.leftOuterJoin(rdd42)
    //        println(res4.collect().mkString(","))

    /**
      * 1.只合并右集合中所有k ,左集合中存在则添加Some(),没有则添加None
      * (4,(Some(41),42)),(1,(None,aaa)),(2,(None,22)),(3,(Some(ccc),ccc))
      */
    //    println("==================rightOuterJoin============")
    //    val rdd41 = sc.parallelize(Array((5, "aaa"), (7, "bbb"), (3, "ccc"), (4, "41")))
    //    val rdd42 = sc.parallelize(Array((1, "aaa"), (2, 22), (3, "ccc"), (4, 42)))
    //    val res4 = rdd41.rightOuterJoin(rdd42)
    //    println(res4.collect().mkString(","))

    /**
      * 1.对两个集合中所有k v 进行合并
      * (4,(Some(41),Some(42))),(1,(None,Some(aaa))),(5,(Some(aaa),None)),(2,(None,Some(22))),(7,(Some(bbb),None)),(3,(Some(ccc),Some(ccc)))
      */
    //    println("==================fullOuterJoin============")
    //    val rdd41 = sc.parallelize(Array((5, "aaa"), (7, "bbb"), (3, "ccc"), (4, "41")))
    //    val rdd42 = sc.parallelize(Array((1, "aaa"), (2, 22), (3, "ccc"), (4, 42)))
    //    val res4 = rdd41.fullOuterJoin(rdd42)
    //    println(res4.collect().mkString(","))

    /**
      * 1. 将单个集合中的k v  聚合，然后与另一集合聚合
      * 2. 两个集合中所有k都进行聚合 与join 不同
      */
    //    println("=============cogroup==============")
    //    val rdd12 = sc.parallelize(Array((3, "a"), (6, "ac"), (2, "b"), (1, "dd")))
    //    val rdd13 = sc.parallelize(Array((1, 3), (3, "3a"), (2, "b"), (1, "dd")))
    //    val res12 = rdd12.cogroup(rdd13)
    //    println(res12.collect.mkString(","))
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

    //    println("=========combineByKey============")
    //    val rdd7 = sc.parallelize(Array(("a", 4), ("a", 5), ("c", 5), ("d", 2), ("c", 6)))
    //    val res7 = rdd7.combineByKey((v) => (v, 1), (u: (Int, Int), v) => (u._1 + v, u._2 + 1),
    //      (c1: (Int, Int), c2: (Int, Int)) => (c1._1 + c2._1, c2._2 + c2._2))
    //    val avg1 = res7.map(item=>(item._1,item._2._1/item._2._2.toDouble))
    //    println(avg1.collect().mkString(","))
    //    println(res7.collect().mkString(","))

    //    println("=========combineByKey============")
    //    val rdd8 = sc.parallelize(Array(("a", 4), ("a", 5), ("c", 5), ("d", 2), ("c", 6)))
    //    val res8 = rdd8.aggregateByKey((0, 1))((u: (Int, Int), v: Int) => (u._1 + v, u._2 + 1),
    //      (u1: (Int, Int), u2: (Int, Int)) => (u1._1 + u2._1,u2._2 + u2._2))
    //    val avg2 = res8.map(item=>(item._1,item._2._1/item._2._2.toDouble))
    //    println(avg2.collect.mkString(","))
    //    println("============aggregateByKey找最大值==========")
    //    val rdd9 = sc.parallelize(Array(("a", 4), ("a", 5), ("c", 5), ("d", 2), ("c", 6)))
    //    val max = rdd9.aggregateByKey(0)((u:Int,v:Int)=>math.max(u,v),(u1:Int,u2:Int)=>math.max(u1,u2))
    //    println(max.collect.mkString(","))

    //    println("============foldByKey找最大值==========")
    //    val rdd10 = sc.parallelize(Array(("a", 4), ("a", 5), ("c", 5), ("d", 2), ("c", 6)))
    //    val max1 = rdd10.foldByKey(0)((u: Int, v: Int) => math.max(u, v))
    //    println(max1.collect.mkString(","))
    /**
      * 按照key 升序排
      */
    //    println("============sortByKey==========")
    //    val rdd11 = sc.parallelize(Array((3, "a"), (6, "ac"), (2, "b"), (1, "dd")))
    //    val res11 = rdd11.sortBy(_._2) //rdd11.sortByKey()
    //    println(res11.collect.mkString(","))
    /**
      * 1.缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。
      * 2.新分区数必须低于原分区数,产生新RDD
      */
    //    println("=============coalesce===============")
    //    val rdd13 = sc.parallelize(Array((3, "a"), (6, "ac"), (2, "b"), (1, "dd")), 5)
    //    println(rdd13.getNumPartitions)
    //    val res13 = rdd13.coalesce(4)
    //    println(rdd13.collect().mkString(","))
    //    println(res13.getNumPartitions)

    /**
      * 1.重新分区，会发生网络混洗。分区可不之前多
      * 2.实际上调用 coalesce(numPartitions, shuffle = true)
      */
//    println("=============repartition===============")
//    val rdd13 = sc.parallelize(Array((3, "a"), (6, "ac"), (2, "b"), (1, "dd")), 5)
//    println(rdd13.getNumPartitions)
//    val res13 = rdd13.repartition(6)
//    println(rdd13.collect().mkString(","))
//    println(res13.getNumPartitions)

    /**
      * 在给定的partitioner内部进行排序(升序)，性能比repartition要高
      */
//    println("===repartitionAndSortWithinPartitions========")
//    val rdd14 = sc.parallelize(Array((3, "d"), (6, "ac"), (2, "a"), (1, "dd")), 5)
//    println(rdd14.getNumPartitions)
//    val res14 = rdd14.repartitionAndSortWithinPartitions(new HashPartitioner(1))
//    println(res14.collect().mkString(","))
//    println(res14.getNumPartitions)

    /**
      *  将各分区数据合并为数组
      */
//    println("===========glom===================")
//    val rdd15 = sc.makeRDD(1 to 16, 4)
//   val res15 = rdd15.glom()
//    println(res15.collect.mkString(","))

    /**
      * 对value 进行处理 key 保持不变
      */
    println("============mapValues===============")
    val rdd16 = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))
    val res16 = rdd16.mapValues(_+1)
    println(res16.collect.mkString(","))

  }


}
