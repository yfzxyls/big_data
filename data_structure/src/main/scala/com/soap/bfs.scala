package com.soap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class note {

  var x: Int = 0
  var y: Int = 0
  var s: Int = 0

}

object note {

  def main(args: Array[String]): Unit = {

    //目标点
    val target = (7, 9)
    //初始化地图大小不超过 50 * 50
    val que: ArrayBuffer[note] = new mutable.ArrayBuffer[note]()
    var head, tail = 0
    //创建l多维数组,记录当前地图坐标
    val a = Array.ofDim[Int](25, 25)
    //标记某一点是否已经到达
    val book = Array.ofDim[Int](25, 25)
    //下一个点需要的点
    val next = Array((0, 1), (1, 0), (0, -1), (-1, 0))
    head = 1
    tail = 1
    que(tail).x = 1
    que(tail).y = 1
    que(tail).s = 0
    //记录下一可能到达的坐标
    var tx, ty = 0
    //导入实现continue的包
    import util.control.Breaks._
    //标记是否到达终点，0未到达，1到达
    var flag = 0
    while (head < tail) {
      for (i <- next) {
        tx = que(head).x + i._1
        ty = que(head).y + i._2
        breakable {
          if (tx < 1 || tx > 25 || ty < 1 || ty > 25) {
            break
          }
        }
        if (a(tx)(ty) == 0 && book(tx)(ty) == 0) {
          book(tx)(ty) = 1
          que(tail).x = tx
          que(tail).y = ty
          que(tail).s = que(head).s + 1
          tail += 1
        }
        //到达目的地
        if (tx == target._1 && ty == target._2) {
          flag = 1
          break()
        }
        head +=1
      }
    }
  }
}

