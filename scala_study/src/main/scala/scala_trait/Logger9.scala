package scala_trait

/**
  * Created by soap on 2018/3/31.
  */
trait Logger9 {
  def log(msg: String)
}

trait ConsoleLogger9 extends Logger9 {
  def log(msg: String) {
    println(msg)
  }
}

trait ShortLogger9 extends Logger9 {
  val maxLength: Int

  abstract override def log(msg: String) {
    super.log(if (msg.length <= maxLength) msg else s"${msg.substring(0, maxLength - 3)}...")
  }
}

class Account9 {
  protected var balance = 0.0
}

abstract class SavingsAccount9 extends Account9 with Logger9 {
  var interest = 0.0

  def withdraw(amount: Double) {
    if (amount > balance) log("余额不足")
    else balance -= amount
  }
}