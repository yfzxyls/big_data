package com.soap

object dfs {

  val a : Array[Int] = new Array[Int](10)
  /**
    * 标记
    */
  val book = new Array[Int](10)

  val n : Int = 0

  def genDFS(step: Int) {
    if (step == n+1){
      a.foreach(print(_))
      return
    }

    var i = 0
    for (i <- 1 to n){
      if(book(i) == 0){}
    }

  }


  def main(args: Array[String]): Unit = {

  }
}
