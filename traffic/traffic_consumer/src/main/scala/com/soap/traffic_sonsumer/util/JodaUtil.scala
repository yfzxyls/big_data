package com.soap.traffic_sonsumer.util

import org.joda.time.DateTime

/**
  * Created by soap on 2018/4/11.
  */
object JodaUtil {
  def main(args: Array[String]): Unit = {

    val date = DateTime.now.toString("yyyyMMdd")
    println(date)
  }
}
