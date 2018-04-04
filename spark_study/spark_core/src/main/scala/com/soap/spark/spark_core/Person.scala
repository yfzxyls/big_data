package com.soap.spark.spark_core

/**
  * Created by soap on 2018/4/4.
  */
@SerialVersionUID(1L) class Person(var name: String, var salary: Int) extends Serializable {

  override def toString = s"name:$name, salary:$salary"
}
