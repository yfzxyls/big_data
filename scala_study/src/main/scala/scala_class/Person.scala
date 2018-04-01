package scala_class

/**
  * Created by soap on 2018/3/30.
  */
class Person {
  println("父类构造器")
  private var name = ""
  private val age: Int = 0
  var sex: Int = 1
  val firstName = ""
}

class Man extends Person {
  println("子类构造器")
}



