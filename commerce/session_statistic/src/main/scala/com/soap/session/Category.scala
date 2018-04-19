package com.soap.session

case class Category(click: Long, order: Long, pay: Long) extends Ordered[Category] {
  override def compare(that: Category): Int = {
    if (this.click - that.click != 0) {
      return (this.click - that.click).toInt
    } else if (this.order - that.order != 0) {
      return (this.order - that.order).toInt
    } else (this.pay - that.pay).toInt
  }
}
