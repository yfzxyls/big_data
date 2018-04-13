package com.soap.traffic.train

import com.soap.traffic.train.utils.RedisPool
import org.apache.hadoop.io.file.tfile.ByteArray

object TestRedis {
  def main(args: Array[String]): Unit = {

    val redisConn = RedisPool.apply("hadoop200").pool.getResource
    val res = redisConn.hgetAll("20180413_0003")

    println(res)


  }

}
