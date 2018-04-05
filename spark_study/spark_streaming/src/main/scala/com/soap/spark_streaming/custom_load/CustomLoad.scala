package com.soap.spark_streaming.custom_load

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import com.soap.spark_streaming.world_count.WorldCount.loadFromHDFS
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
  * Created by soap on 2018/4/5.
  */
class CustomLoad(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK){
  //
  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  private def receive() {
    var socket: Socket = null
    var userInput: String = null
    try {
      // Connect to host:port
      socket = new Socket(host, port)

      // Until stopped or connection broken continue reading
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
      userInput = reader.readLine()
      while(!isStopped && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Exception =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
  //
  override def onStop(): Unit = {}
}

object CustomLoad{
  def main(args: Array[String]): Unit = {
    /**
      * 需要长期运行的接收器，因此必须多线程
      */
    val sc = new SparkConf().setAppName("streaming_word_count").setMaster("local[*]")
    //优雅停机
    //    sc.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sc, Seconds(5))
    /**
      * 使用自定义数据输入
      * 监听失败支持重试
      */
    val input = ssc.receiverStream(new CustomLoad("hadoop200",9999))
    val wordCount = input.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    wordCount.print(100)
    //    loadFormPort(ssc)
    ssc.start()
    ssc.awaitTermination()
  }
}
