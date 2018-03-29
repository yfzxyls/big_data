package base

/**
  * Created by soap on 2018/3/27.
  */
object BaseStudy {

  def main(args: Array[String]): Unit = {

    val a = BigInt(5)./%(2)
    println(a)

    //          *           i
    //         ***
    //        *****
    //       *******
    //     **********

    //打印正三角形
    //    for (i <- 1 to 10) {
    //     for (j <- i to 10) {
    //        print(" ")
    //      }
    //      for (k <- 1 to i) {
    //        print("*")
    //        print(" ")
    //      }
    //      println()
    //    }


    

    println("===========异常========")

    def f8(a: Int, b: Int) = {
      if (b == 0) throw new RuntimeException("除数为0")
      a / b
    }
    //println(f8(10, 0))
//    try {
//      println(f8(10, 0))
//    } catch {
//      case e: Exception => print("捕获异常：" + e.getMessage)
//    } finally {
//      println("执行finally")
//    }

  }
}
