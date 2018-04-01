package scala_trait

/**
  * Created by soap on 2018/3/31.
  */
trait Logger1 {
  def log(msg: String)
}

class ConsoleLogger1 extends Logger1 with Cloneable with Serializable {
  def log(msg: String) {
    println(msg)
  }
}
