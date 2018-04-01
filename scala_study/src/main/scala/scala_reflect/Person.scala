package scala_reflect

/**
  * Created by soap on 2018/3/31.
  */
class Person(name:String, age: Int) {
  def myPrint() = {
    println(name + "," + age)
  }


  override def toString = s"Person($name,$age)"
}
