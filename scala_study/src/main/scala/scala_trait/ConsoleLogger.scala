package scala_trait

/**
  * Created by soap on 2018/3/31.
  */
trait ConsoleLogger {
  def log(msg: String) {
    println(msg)
  }
}

class Account {
  protected var balance = 0.0
}

class SavingsAccount extends Account with ConsoleLogger {
  def withdraw(amount: Double) {
    if (amount > balance) log("余额不足")
    else balance -= amount
  }
}