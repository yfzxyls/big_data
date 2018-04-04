package com.soap.spark.accumulator

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by soap on 2018/4/4.
  */
class LogAccumulator extends org.apache.spark.util.AccumulatorV2[String, mutable.Set[String]] {
  private val _logArray: mutable.Set[String] = mutable.HashSet[String]()

  override def isZero: Boolean = {
    _logArray.isEmpty
  }

  override def reset(): Unit = {
    _logArray.clear()
  }

  override def add(v: String): Unit = {
    _logArray.add(v)
  }

  override def merge(other: org.apache.spark.util.AccumulatorV2[String, mutable.Set[String]]): Unit = {
    other match {
      case o: LogAccumulator => _logArray.++=(o.value)
    }

  }

  override def value: mutable.Set[String] = {
    _logArray
  }

  override def copy(): org.apache.spark.util.AccumulatorV2[String, mutable.Set[String]] = {
    val newAcc = new LogAccumulator()
    _logArray.synchronized {
      newAcc._logArray.++=(_logArray)
    }
    newAcc
  }


}

object LogAccumulator {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LogAccumulator") .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val accum = new LogAccumulator
    sc.register(accum, "logAccum")
    val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2).filter(line => {
      val pattern = """^-?(\d+)"""
      val flag = line.matches(pattern)
      if (!flag) {
        accum.add(line)
      }
      flag
    }).map(_.toInt).reduce(_ + _)

    println("sum: " + sum)
    for (v <- accum.value) print(v + "")
    println()
    sc.stop()
  }
}