package scala_class

import scala.collection.mutable.ArrayBuffer

/**
  * Created by soap on 2018/3/30.
  */
class NetWork {

  val members = new ArrayBuffer[Member]()

  class Member(name:String) {
    val contacts = new ArrayBuffer[NetWork#Member]() //类型投影

    override def toString = s"Member($name)"
  }

  def join(name: String) = {
    val m = new Member(name)
    members += m
    m
  }

}
