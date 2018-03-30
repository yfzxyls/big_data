package scala_class

/**
  * Created by soap on 2018/3/30.
  */
class SingletonClass(name: String) {

  //TODO:类可以访问伴生类对象的使用属性,并不是相互访问
  val age: Int = SingletonClass.play()

  override def toString = s"$name"


}

object SingletonClass {
  private var instance: SingletonClass = null

  def apply(name: String): SingletonClass = {
    println("伴生类对象")
    if (instance == null) {
      println("伴生类对象")
      instance = new SingletonClass(name)
    }
    instance
  }

  private def play() = {
    2
  }
}
