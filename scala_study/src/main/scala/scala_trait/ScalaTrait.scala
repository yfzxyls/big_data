package scala_trait

/**
  * Created by soap on 2018/3/31.
  */
object ScalaTrait {

  def main(args: Array[String]): Unit = {
    /**
      * 1.extends 后部分为一个整体，可以多with
      * 2.只实现一个特质时使用extends
      */
    val loger = new ConsoleLogger1
    loger.log("ConsoleLogger")

    println("===============特质中带具体实现=========")
    val savingsAccount = new SavingsAccount()
    savingsAccount.withdraw(2.0)

    println("============动态多混入========")
    /**
      * 1.动态混入多特质时，从右向左调用
      * 2.如果不想调用左边特质方法，则可使用 super[ConsoleLogger5].log 显示指定调用父类
      */
    val acct1 = new SavingsAccount5 with TimestampLogger5 with ShortLogger5
    val acct2 = new SavingsAccount5 with ShortLogger5 with TimestampLogger5
    acct1.withdraw(100)
    acct2.withdraw(100)

    println("=======在特质中重写抽象方法========")
    //这里可以根据12.5的知识点理解此处
    val acct31 = new SavingsAccount6 with ConsoleLogger6 with TimestampLogger6 with ShortLogger6
    val acct32 = new SavingsAccount6 with ConsoleLogger6 with ShortLogger6 with TimestampLogger6
    acct31.withdraw(100)
    acct32.withdraw(100)

    println("==========当做富接口使用的特质=======")
    val acct7 = new SavingsAccount7 with ConsoleLogger7
    acct7.withdraw(100)

    println("=============特质中的具体字段========")
    /**
      *1.动态混入时，其中一个特质中包含的集体自动，其他混入特质中不能重复
      */
    val acct8 = new SavingsAccount8
    acct8.withdraw(100)
    println(acct8.maxLength)

    println("============特质中的抽象字段============")

    /**
      * 匿名实现抽象
      */
    val acct9 = new SavingsAccount9 with ConsoleLogger9 with ShortLogger9 {
      override val maxLength: Int = 20
    }
    acct9.withdraw(100)
    println(acct9.maxLength)

    println("=========== 特质构造顺序============")
    /**
      * 首先调用父类构造器，有多个子类实现父类，父类构造器仍调用一次
      */
    val acct10 = new SavingsAccount10 with ConsoleLogger10 with ShortLogger10
    acct10.withdraw(100)
    println(acct10.maxLength)
  }
}
