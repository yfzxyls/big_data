package function

/**
  * Created by soap on 2018/3/30.
  */
object HightFunction {
  def main(args: Array[String]): Unit = {
    println("==============柯里化函数==========")
    /**
      * 匿名函数
      *
      */
    val f1 = (x: Int) => {
      x * 2
    }
    println(f1(2))

    /**
      * 高阶函数，参数可以是函数
      */
    def f2(f: Int => Int) = {
      f(2)
    }

    val res0 = f2 { x => x + 1 }
    println(res0)

    /**
      * 将函数赋值给另一个函数
      */
    def f5(x: Int) = (y: Int) => x + y

    println(f5(2)(2))


    def f6(x: (Int, String) => String) = Unit

    def f61(x: Int, y: String) = {
      println(x + y)
      x + y
    }

    println(f61(1, "2"))

    //    3、定义一个高阶函数，能够接收一个函数，接收的函数：只有一个形参，并且是Int类型，并且返回String。
    //    高阶函数返回用户传递的函数。
    def f7(x: (Int) => String) = x

    //
    //    4、定一个高阶函数，能够接收两个函数
    //    接收的第一个函数：只有一个形参，Int类型，返回值为传递进来的Int类型的值.toString
    //    接收的第二个函数：有两个形参，分别是Int类型和String类型，返回值为该Int
    //    高阶函数返回第一个函数和第二个函数运算的结果的拼接。
    def f8(x: (Int) => String, y: (Int, String) => Int) = {
      x(10) + y(20, "String")
    }

    def f9(x: Int) = (y: Int) => x + y

    def f10(x: Int)(y: Int) = x + y

    /**
      * 高阶函数传多个值 
      */
    def f3(x: Int, y: Int => Int) = {
      y(x) + 1
    }

    println(f3(2, x => x - 1))

    /**
      * 函数柯里化
      */

    def f4(x: Int, y: Int) = x + y

    println("============高阶函数使用==========")
    println(f4(1, 3))
    val a = Array("Hello", "World")
    val b = Array("hello", "world")
    println(a.corresponds(b)(_.equalsIgnoreCase(_)))

    println("=============控制抽象============")

    /**
      * 1. 满足f1 结构的函数都会调用
      */

    def runInThread(f1: () => Unit): Unit = {
      new Thread {
        override def run(): Unit = {
          f1()
        }
      }.start()
    }

    runInThread( () =>{
      println("1" + Thread.currentThread().getName)
     // () =>
        println("干活咯！")
        println("2" + Thread.currentThread().getName)
        Thread.sleep(5000)
        println("3" + Thread.currentThread().getName)
        () =>
          println("干完咯！")
          println("4" + Thread.currentThread().getName)
    })

  }
}
