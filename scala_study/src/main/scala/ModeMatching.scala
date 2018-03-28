/**
  * Created by soap on 2018/3/28.
  */
object ModeMatching {

  def main(args: Array[String]): Unit = {
    println("==========Switch==========")
    /**
      * 1.必须有一个case 能够匹配,否则会报错 可以用 _ 表示匹配任意
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


  }

}
