package scala_class

/**
  * Created by soap on 2018/3/30.
  */
class Cat1 {
  val hair = Cat1.growHair
  private var name = ""

  def changeName(name: String) = {
    this.name = name
  }

  def describe = println("hair:" + hair + "name:" + name)
}

object Cat1 {
  private var hair = 0

  private def growHair = {
    hair += 1
    hair
  }
}
