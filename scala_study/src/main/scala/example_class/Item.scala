package example_class

/**
  * Created by soap on 2018/3/29.
  */

abstract class Item

  case class Article(description: String, price: Double) extends Item

  case class Bundle(description: String, discount: Double, item: Item*) extends Item


