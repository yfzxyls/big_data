package com.soap.flink.source

import org.apache.flink.api.scala._


/**
  * @author yangfuzhao on 2019/1/7. 
  */
object aggApi extends App {

  override def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements((2,2),(2,2),(4,1),(2,5),(3,5),(2,5),(2,1))
//    val dataSet = env.fromElements((2,3,2),(2,1,2),(3,7,4),(2,5,2),(4,5,6),(2,5,7),(1,2,2))
//    val dataSet = env.fromElements(1,2,2,2,1,1,2,5,1,5,2,5,2,0)

//    dataSet.min(0).print()
//    dataSet.min(1).print()
//    dataSet.min(0).print()
//    dataSet.minBy(0).print()
    


    dataSet.max(0).print()
    dataSet.maxBy(0).print()
//    dataSet.sum(0).print()

  }
}
