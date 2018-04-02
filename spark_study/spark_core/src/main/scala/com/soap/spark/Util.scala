package com.soap.spark

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

/**
  * Created by soap on 2018/4/2.
  */
object Util {
  def main(args: Array[String]): Unit = {

    //15/Feb/2017:01:42:32
    val sdf = new SimpleDateFormat("dd/MMM/yyyy:HH;mm:ss",Locale.CANADA)
    val dateStr = sdf.parse("15/Feb/2017:01:42:32")
    println(dateStr)

  }
}
