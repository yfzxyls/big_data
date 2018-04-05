package com.soap.spark_sql.utils

import java.util
import java.util.{HashMap, Map, Properties}

/**
  * Created by soap on 2018/4/5.
  */
object PropertiesLoader {

  val properties: java.util.Properties = new java.util.Properties()
  private val propertiesMap: util.Map[String, Properties] = new util.HashMap[String, Properties]

  def loaderProperties(path: String): Unit = {
    if (propertiesMap.get(path) == null) {
      val classLoader = Thread.currentThread().getContextClassLoader
      properties.load(classLoader.getResourceAsStream("db.properties"))
      propertiesMap.put(path, properties)
    }
  }

}
