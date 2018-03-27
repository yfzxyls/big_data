/**
  * Created by soap on 2018/3/27.
  */
object Loop {
  def main(args: Array[String]): Unit = {
    //while 循环
    var n = 0
    val wh = while (n <= 10) {
      n += 1
    }
    println(n)

    //while 循环中断

    import util.control.Breaks

    val loop = new Breaks
    loop.breakable {
      var i = 0
      while (true) {
        i += 1
        if (i > 5) {
          println(i)
          loop.break()
        }
      }
    }
    println("=========for循环==========")
    //for循环
    //    for (i <- 1 to 10; j <- Range(10, 1, -1)) {
    //      println(i + "_" + j)
    //    }
    println("=========for循环判断式==========")
    //条件判断式
    for (i <- 1 to 5 if i != 3; j <- Range(5, 2, -2)) {
      println(i + "_" + j)
    }

    println("=========for循环引入变量==========")
    /**
      * 每次循环赋值一次
      */
    for (i <- 1.to(3); k = i + 2; j <- Range(5, 2, -2)) {
      println(i + "_" + k + "_" + j)
    }

    println("=========for yield==========")
    /**
      * 将循环仲基值保存在向量中返回
      * yield 后只能接一行代码或者代码块
      */
    val a = for (j <- Range(5, 1, -1); i <- 1 to 3) yield {
      j + i
    }
    println(a)
    println("=========for 使用{} 代替()==========")

    for {i <- 1 to 8} {
      print(i+",")
    }

  }


}
