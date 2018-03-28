package unit6

/**
  * Created by soap on 2018/3/28.
  * 样例类学习
  */
object ExampleClass {

  def main(args: Array[String]): Unit = {

    /**
      * 1.先根据类型匹配，匹配对象没有参数，则只匹配类型，否则调用equlse
      */

    for (amt <- Array(Dollar(1000.0), Currency(1000.0, "EUR"), Nothing)) {
      val result = amt match {
        case Dollar(1000) => "$"
        case Currency(_, u) => u
        case Nothing => ""
        case _ => "default"
      }
      println(amt + ": " + result)
      //Dollar(1000.0): $1000.0
      //Currency(1000.0, "EUR"): EUR
      //Nothing:
    }
  }
}
