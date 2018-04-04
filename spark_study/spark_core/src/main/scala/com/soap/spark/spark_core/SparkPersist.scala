package com.soap.spark.spark_core

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by soap on 2018/4/3.
  */
object SparkPersist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("persist").setMaster("local[*]")
    //.setMaster("local[*]")
    //创建SparkContext
    //TODO:设置hadoop用户，不生效，在环境变量中添加HADOOP_USER_NAME = hadoop 重启idea
//    sparkConf.set("spark.hadoop.user.name","hadoop")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(1 to 10)
    val nocache = rdd.map(_.toString + "[" + System.currentTimeMillis + "]")
//    nocache.cache()
    //StorageLevel.MEMORY_ONLY 默认
    //nocache.persist(StorageLevel.DISK_ONLY_2)

    //设置检查点保存位置
    sc.setCheckpointDir("hdfs://hadoop200:9000/spark/checkpoint")
//    rdd.checkpoint()


    /**
      * 1.不缓存时间时，每次action 操作会执行所有的 rdd 转换操作
      *
      */
//    println("==============不缓存数据==============")
    nocache.checkpoint()
    println(nocache.collect().mkString(","))
    //1[1522754352995],2[1522754352995],3[1522754353005],4[1522754353005],5[1522754353005],6[1522754353006],7[1522754353006],8[1522754353004],9[1522754353004],10[1522754353004]
    println(nocache.collect().mkString(","))
    //1[1522754353137],2[1522754353137],3[1522754353145],4[1522754353145],5[1522754353145],6[1522754353147],7[1522754353147],8[1522754353127],9[1522754353127],10[1522754353127]
    println(nocache.collect().mkString(","))
    println(nocache.collect().mkString(","))
  }

}
