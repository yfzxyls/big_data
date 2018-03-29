package example_class

/**
  * Created by soap on 2018/3/29.
  */
sealed abstract class TrafficLightColor
case object Red extends TrafficLightColor
case object Yellow extends TrafficLightColor
case object Green extends TrafficLightColor
