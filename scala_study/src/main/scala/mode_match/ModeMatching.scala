package mode_match

/**
  * Created by soap on 2018/3/28.
  */
object ModeMatching {

  def main(args: Array[String]): Unit = {
    println("==========Switch==========")
    /**
      * 1.必须有一个case 能够匹配,否则会报错 可以用 _ 表示匹配任意
      * 2.类型匹配 必须有变量接收匹配结果 c: Int  不能使用 Int
      */

    val op: Char = ')'
    op match {
      case '+' => println("+")
      case '-' => println("-")
      case '*' => println("*")
      case '_' => println("_")
      case tmp@_ => println(tmp)
    }

    println("==========Switch 守卫==========")
    for (c <- ("&8@(#&^%")) {
      c match {
        case '&' => println("&")
        case '-' => println("-")
        case '*' => println("*")
        case '@' => println("@")
        case '#' => println("#")
        case tmp@_ if Character.isDigit(c) => println("数字：" + tmp)
        case tmp@_ => println(tmp)
      }
    }

    println("==========Switch 类型匹配==========")
    /**
      * 1.除数组的类型,其他范型将被擦出，
      */
    val a = 3
    val obj = if (a == 1) 1
    else if (a == 2) "2"
    else if (a == 3) BigInt(3)
    else if (a == 4) Map("aa" -> 1)
    else if (a == 5) Map(1 -> "aa")
    else if (a == 6) Array(1, 2, 3)
    else if (a == 7) Array("aa", 1)
    else if (a == 8) Array("aa")

    val r1 = obj match {
      case x: Int => x
      case s: String => s.toInt
      case x: BigInt => -1 //不能这么匹配
      case _: BigInt => Int.MaxValue
      case m: Map[String, Int] => "Map[String, Int]类型的Map集合"
      case m: Map[_, _] => "Map集合"
      case a: Array[Int] => "It's an Array[Int]"
      case a: Array[String] => "It's an Array[String]"
      case a: Array[_] => "It's an array of something other than Int"
      case _ => 0
    }
    println(r1 + ", " + r1.getClass.getName)

    println("=========匹配数组、列表、元组===========")
    /**
      * 1.Array(0) 匹配有且仅有一个0的数组
      * 2.Array(x, y) 匹配有两个元素的数组并将匹配值传递给x y
      * 3.Array(0, _*) 匹配一0开头的数组
      */

    for (arr <- Array(Array(0), Array(1, 0), Array(0, 1, 0), Array(1, 1, 0), Array(1, 1, 0, 1))) {
      val result = arr match {
        case Array(0) => "0"
        case Array(x, y) => x + " " + y
        case Array(x, y, z) => x + " " + y + " " + z
        case Array(0, _*) => "0..."
        case _ => "something else"
      }
      println(result)
    }

    println("=========匹配列表===========")
    /**
      * 1.0 :: a 匹配一0开头的列表 并将0以后的值组成列传递给a 
      */
    for (lst <- Array(List(0), List(1, 0), List(0, 0, 0), List(1, 0, 0))) {
      val result = lst match {
        case 0 :: Nil => "0"
        case x :: y :: Nil => x + " " + y
        case 0 :: a => "0 ..." + a
        case _ => "something else"
      }
      println(result)
    }

    println("=========匹配元组=========")
    /**
      * 1.(0,_) 匹配一0开头的元组
      */

    for (pair <- Array((0, 1), (1, 0), (1, 1), (1, 0, 3))) {
      val result = pair match {
        case (0, _) => "0 ..."
        case (y, 0) => y + " 0"
        case (y, 0, z) => y + " 0" + z
        case _ => "neither is 0"
      }
      println(result)
    }

    println("=======提取器==========")
    /**
      * 1. 模式匹配会先调用unapply 将返回值用于匹配
      */
    val number: Double = 36.0
    number match {
      case Square(2) => println(s"square root of $number is ")
      case _ => println("nothing matched")
    }
    println("============unapplySeq============")
    /**
      * 1. Seq 类型使用 unapplySeq 提取后进行匹配
      */
    val namesString = "Alice,Bob,Thomas"
    namesString match {
      case Names(first, second, third) => {
        println("the string contains three people's names")
        println(s"$first $second $third")
      }
      case _ => println("nothing matched")
    }

    println("============ for表达式中的模式匹配==============")
    /**
      * 1.for 中可以没有匹配项
      */

    import scala.collection.JavaConverters._
    for ((k, v) <- System.getProperties.asScala)
      println(k + " -> " + v)

    //匹配值为 空
    for ((k, v) <- System.getProperties.asScala) {
      println(k)
    }

    //使用守卫匹配值为空
    for ((k, v) <- System.getProperties.asScala if v.length == 2)
      println(k + " = " + v)


  }
}
