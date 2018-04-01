package scala_trait

/**
  * Created by soap on 2018/3/31.
  */
trait Logger4 {
  def log(msg: String)
}

trait ConsoleLogger4 extends Logger4 {
  def log(msg: String) {
    println(msg)
  }
}

class Account4 {
  protected var balance = 0.0
}

abstract class SavingsAccount4 extends Account4 with Logger4 {
  def withdraw(amount: Double) {
    if (amount > balance) log("余额不足")
    else balance -= amount
  }
}

object Main4 extends App {
  /**
    * 1.动态混入已实现的特质
    */
  val account = new SavingsAccount4 with ConsoleLogger4
  account.withdraw(100)
}