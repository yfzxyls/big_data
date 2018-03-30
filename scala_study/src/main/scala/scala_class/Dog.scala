package scala_class

import scala.beans.BeanProperty

/**
  * Created by soap on 2018/3/30.
  */
class Dog {

  val eyes = 2
  //
  @BeanProperty var leg = 4


  override def toString = s"Dog($eyes, $leg)"
}

class Cat private(val name: String) {
  println("主构造器执行")
  var age: Int = 0

  def this(age: Int, name: String) {
    this(name)
    this.age = age
  }

  def this(age: Int, name: String,sex : Int) {
    this(age,name)
    this.age = age
  }

}

object Cat {
  def cat(name: String): Cat = {
    new Cat(name)
  }
}
