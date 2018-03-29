package example_class

/**
  * Created by soap on 2018/3/28.
  * 样例类
  */
abstract class Amount

  case class Dollar(value: Double) extends Amount

  case class Currency(value: Double, unit: String) extends Amount

  case object Nothing extends Amount
