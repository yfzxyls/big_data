package com.soap.spark_sql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by soap on 2018/4/4.
  */
case class Employee(name: String, salary: Long)
case class Average(var sum: Long, var count: Long)
object MyAverage extends Aggregator[Employee,Average,Double]{
  // 定义一个数据结构，保存工资总数和工资总个数，初始都为0
  override def zero: Average = Average(0L,0L)

  override def reduce(b: Average, a: Employee): Average = {
    b.count += a.salary
    b.sum += 1
    b
  }
  // 聚合不同execute的结果
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }
  // 计算输出
  override def finish(reduction: Average): Double = {
    reduction.count.toDouble/reduction.sum
  }
  // 设定之间值类型的编码器，要转换成case类
  override def bufferEncoder: Encoder[Average] = Encoders.product
  // 设定最终输出值的编码器
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
