package scala_class

/**
  * Created by soap on 2018/3/30.
  */
class Creature {
  val range: Int = 10
  val env: Array[Int] = new Array[Int](range)
}

//class Ant extends Creature {
//  override val range = 2
// // override val env: Array[Int] = new Array[Int](range)
//}

class Ant extends {
  override val range = 2
  // override val env: Array[Int] = new Array[Int](range)
}with Creature