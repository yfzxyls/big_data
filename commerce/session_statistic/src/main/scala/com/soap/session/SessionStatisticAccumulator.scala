package com.soap.session

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable


class SessionStatisticAccumulator extends AccumulatorV2[String, mutable.Map[String, Int]] {

  val accMap = new mutable.HashMap[String, Int]()

  override def isZero: Boolean = accMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    val newAccMap = new SessionStatisticAccumulator()
    newAccMap.accMap ++= accMap
    newAccMap
  }


  override def reset(): Unit = {
    accMap.clear()
  }

  override def add(v: String): Unit = {
    if (!accMap.contains(v)) accMap += (v -> 0)
    accMap.update(v, accMap(v) + 1)
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    other match {
      case acc: SessionStatisticAccumulator =>
        acc.accMap.foldLeft(this.accMap){
          case (map,(k,v)) => map += (k -> (map.getOrElse(k, 0) + v))
        }
//        (this.accMap /: acc.accMap) {
//          case (map, (k, v)) => map += (k -> (map.getOrElse(k, 0) + v))
//        }
      case _ =>
    }
  }

  override def value: mutable.Map[String, Int] = {
    this.accMap
  }
}
