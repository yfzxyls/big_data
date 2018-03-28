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
  }
}
