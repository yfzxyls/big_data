package scala_trait

/**
  * Created by soap on 2018/3/31.
  */
trait Logger8 {
  
  def log(msg: String)
}

trait ConsoleLogger8 extends Logger8 {
  def log(msg: String) {
    println(msg)
  }
}

trait ShortLogger8 extends Logger8 {
  val maxLength = 15
  abstract override def log(msg: String) {
    super.log(if (msg.length <= maxLength) msg else s"${msg.substring(0, maxLength - 3)}...")
  }
}

class Account8 {
 
  protected var balance = 0.0
}

class SavingsAccount8 extends Account8 with ConsoleLogger8 with ShortLogger8 {
  var interest = 0.0
  
  def withdraw(amount: Double) {
    if (amount > balance) log("余额不足")
    else balance -= amount
  }
}
