package scala_trait

/**
  * Created by soap on 2018/3/31.
  */
trait Logger10 {
  println("我在Logger10特质构造器中，嘿嘿嘿。。。")
  def log(msg: String)
}

trait ConsoleLogger10 extends Logger10 {
  println("我在ConsoleLogger10特质构造器中，嘿嘿嘿。。。")
  def log(msg: String) {
    println(msg)
  }
}

trait ShortLogger10 extends Logger10 {
  val maxLength: Int
  println("我在ShortLogger10特质构造器中，嘿嘿嘿。。。")

  abstract override def log(msg: String) {
    super.log(if (msg.length <= maxLength) msg else s"${msg.substring(0, maxLength - 3)}...")
  }
}

class Account10 {
  println("我在Account10构造器中，嘿嘿嘿。。。")
  protected var balance = 0.0
}

abstract class SavingsAccount10 extends Account10 with ConsoleLogger10 with ShortLogger10{
  println("我再SavingsAccount10构造器中")
  var interest = 0.0
  override val maxLength: Int = 20
  def withdraw(amount: Double) {
    if (amount > balance) log("余额不足")
    else balance -= amount
  }
}
