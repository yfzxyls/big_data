package data_structure

import scala.collection.mutable.ArrayBuffer

/**
  * Created by soap on 2018/3/29.
  */
object ScalaArray {
  def main(args: Array[String]): Unit = {

    println("===========定长数组=========")
    /**
      * 1.定长数组初始化必须指定长度
      */

    val array = new Array[Int](5)
    val array1 = new Array[Int](6)

    println(array.mkString(","))
    println(array(1) = 1)
    println(array.mkString("a", "b", "c"))

    println("===========变长数组=========")
    /**
      * 1.变长数组初始化可以不指定长度
      * 2.remove(0) 删除序号为0的值并返回
      * remove(0,2) 从序号0开始删除 ，删除两个元素
      * 删除序号不能超过原数组下标，总数不能超过
      */
    val buffArr = new ArrayBuffer[String]()
    buffArr.append("a")
    buffArr.append("b")
    buffArr.append("c")
    println(buffArr.mkString(","))
    //    val re = buffArr.remove(0, 3)
    //    println(re)
    println(buffArr.mkString(","))

    for (x <- buffArr) {
      println("buff" + x)
    }

    println("=============数组方法==========")

    /**
      * 1.sum只能用于数字
      * 2.sorted 可用于所有类型排序 默认升序
      */
    val array2 = Array(12, 8, 5, 4, 5)
    println(array2.sum)
    val res0 = array2.sortWith(_ < _)
    println(res0.mkString(","))
    //    val buffToArr = buffArr.toArray

    println("=========数组遍历===========")
    //逆序遍历
    for (i <- (0 until array2.length).reverse ) {
      println(i + ":" + array2(i))
    }
    println("--------------------------")
    //指定步长遍历
    for (i <- 0 until(array2.length, 2)) {
      println(i + ":" + array2(i))
    }

    println("===========scala转java数组=========")
    val arr4 = ArrayBuffer("1", "2", "3")
    //Scala to Java
    import scala.collection.JavaConversions.bufferAsJavaList
    val javaArr = new ProcessBuilder(arr4)
    println(javaArr.command())
    println("===========java转scala数组=========")
    import scala.collection.JavaConversions.asScalaBuffer
    import scala.collection.mutable.Buffer
    val scalaArr: Buffer[String] = javaArr.command()
    println(scalaArr)

  }
}
