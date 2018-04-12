package com.soap.traffic_sonsumer.util

import java.util.Properties

/**
  * Created by soap on 2018/4/11.
  */
object PropertiesUtil {

  private val properties = new Properties()

  properties.load(ClassLoader.getSystemResourceAsStream("kafka.properties"))

  def getKey(key:String,defaultValue:String = ""):String ={
    properties.getProperty(key,defaultValue)
  }

  def getPeroperties():Properties = {
    properties
  }

}
