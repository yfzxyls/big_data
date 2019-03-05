package com.soap.flink.source.datastream
import java.net.ServerSocket

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.scala._

/**
  * @author yangfuzhao on 2019/1/15.
  */
object SocketService extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val ss = new ServerSocket(9999)
    val s = ss.accept()
    val outputStream = s.getOutputStream
    while (true) {
      Thread.sleep(1000)
      System.currentTimeMillis().toString
      outputStream.write(System.currentTimeMillis().toString.getBytes)
    }
    outputStream.close()
    ss.close()
  }

}
