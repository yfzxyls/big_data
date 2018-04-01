package scala_implicit

/**
  * Created by soap on 2018/3/31.
  */
object StringUtils {
  implicit class StringImprovement(val s : String){ //隐式类
    def increment = s.map(x => (x +1).toChar)
  }
}

