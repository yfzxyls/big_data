package scala_trait

/**
  * Created by soap on 2018/3/31.
  */
trait Logger5 {
  def log(msg: String);
}

trait ConsoleLogger5 extends Logger5 {
  def log(msg: String) {
    println(msg)
  }
}

trait TimestampLogger5 extends ConsoleLogger5 {
  override def log(msg: String) {
    super.log(new java.util.Date() + " " + msg)
  }
}

trait ShortLogger5 extends ConsoleLogger5 {
  override def log(msg: String) {
    super[ConsoleLogger5].log(if (msg.length <= 15) msg else s"${msg.substring(0, 12)}...")
  }
}

class Account5 {
  protected var balance = 0.0
}

abstract class SavingsAccount5 extends Account5 with Logger5 {
  def withdraw(amount: Double) {
    if (amount > balance) log("余额不足,请充值后重试。。。。。1")
    else balance -= amount
  }
}
