package com.soap.spark.cdn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by soap on 2018/4/2.
  */
object CDN {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("cdn").setMaster("local[*]")
    //创建SparkContext
    val sparkContext = new SparkContext(sparkConf)
    val textFile = sparkContext.textFile("D:\\study\\big_data\\spark_study\\spark_core\\src\\main\\resource\\cdn.txt")
    //访问次数
    val ipCilck = textFile.flatMap(_.split("\n")).map(_.split(" "))
    ipCilck.cache()
    val ipCilcks =  ipCilck .map{i=>(i(0),1)}.reduceByKey(_+_).sortBy(_._2)//.take(10)
    println(ipCilcks.collect.mkString(","))

    //video
    //www.ddd/.mp4
    //117.141.38.250 HIT 30 [15/Feb/2017:00:26:46 +0800] "GET http://cdn.v.abc.com.cn/videojs/video.js HTTP/1.1" 200 174511 "http://www.abc.com.cn" "Mozilla/4.0+(compatible;+MSIE+6.0;+Windows+NT+5.1;+Trident/4.0;)"
    val vidoeClick = ipCilck.filter{i=> !".+.mp4$".r.findAllIn(i(6)).isEmpty}.map{i=>{
      val url = i(6)
      val vidoe =url.substring(url.lastIndexOf("/")+1)
      println(vidoe)
      (vidoe,1)
    }}.reduceByKey(_ + _).sortBy(_._2)
    println(vidoeClick.collect().mkString(","))

    ipCilck.map{i=>}


  }
}
