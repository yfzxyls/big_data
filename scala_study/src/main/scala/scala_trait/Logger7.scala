package scala_trait

/**
  * Created by soap on 2018/3/31.
  */
trait Logger7 {
  def log(msg: String)

  def info(msg: String) {
    log("INFO: " + msg)
  }

  def warn(msg: String) {
    log("WARN: " + msg)
  }

  def severe(msg: String) {
    log("SEVERE: " + msg)
  }
}

trait ConsoleLogger7 extends Logger7 {
  def log(msg: String) {
    println(msg)
  }
}

class Account7 {
  protected var balance = 0.0
}

abstract class SavingsAccount7 extends Account7 with Logger7 {
  def withdraw(amount: Double) {
    if (amount > balance) severe("余额不足")
    else balance -= amount
  }
}