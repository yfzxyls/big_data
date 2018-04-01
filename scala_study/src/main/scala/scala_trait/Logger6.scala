package scala_trait

/**
  * Created by soap on 2018/3/31.
  */
trait Logger6 {
  def log(msg: String)
}
trait ShortLogger6 extends Logger6 {
  abstract override def log(msg: String) {
    super.log({
      println("ShortLogger6")
      if (msg.length <= 15) msg else s"${msg.substring(0, 12)}..."
    })
  }
}
//因为有super，Scala认为log还是一个抽象方法
trait TimestampLogger6 extends Logger6 {
  abstract override def log(msg: String) {
    super.log({
      println("TimestampLogger6")
      new java.util.Date() + " " + msg
    })
  }
}



trait ConsoleLogger6 extends Logger6 {
  override def log(msg: String) {
    println("ConsoleLogger6")
    println(msg)
  }
}

class Account6 {
  protected var balance = 0.0
}

abstract class SavingsAccount6 extends Account6 with Logger6 {
  def withdraw(amount: Double) {
    if (amount > balance) log("余额不足，，，，，，，，，，，，，，")
    else balance -= amount
  }
}
