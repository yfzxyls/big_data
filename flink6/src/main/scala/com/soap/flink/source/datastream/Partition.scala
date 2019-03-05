package com.soap.flink.source.datastream

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
/**
  * @author yangfuzhao on 2019/1/21.
  *
  *  1.shuffle 根据channle随机选择
  *  2.rebalance 使用轮询调度算法
  *  3.rescale 本地轮询重新分区
  *  4.broadcast 将元素广播到所有分区
  */
object Partition {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val text1 = env.socketTextStream("localhost", 9999)

    val count1 = text1.flatMap(_.split(" "))
      .filter(!StringUtils.isEmpty(_))
      .map((_, 1))
      //      .keyBy(0)

      //      .shuffle
      //      .rescale
      .broadcast()
    //.partitionCustom(new MyPartition, 0)


    val text2 = env.socketTextStream("localhost", 9998)

    val count2 = text2.flatMap(_.split(" "))
      .filter(!StringUtils.isEmpty(_))
      .map((_, 1))
      .keyBy(0)

    count2.connect(count1).process(new KeyedBroadcastProcessFunction[String, (String, Int), (String, Int), (String, Int)] {


      override def processElement(value: (String, Int), ctx: KeyedBroadcastProcessFunction[String, (String, Int), (String, Int), (String, Int)]#ReadOnlyContext, out: Collector[(String, Int)]): Unit = {
        println("value:" + value)
        println(ctx.getCurrentKey)

        out.collect(value)
      }

      override def processBroadcastElement(value: (String, Int), ctx: KeyedBroadcastProcessFunction[String, (String, Int), (String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
        println("broad:" + value)
        println(ctx.currentWatermark())
        out.collect(value)
      }
    })

    env.execute("partition")


  }

  /**
    * 分区号必须小于整体并行度
    */
  class MyPartition extends Partitioner[String] {
    override def partition(key: String, numPartitions: Int): Int = {
      key.length
    }
  }

}
