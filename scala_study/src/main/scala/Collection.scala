import scala.collection.mutable.ArrayBuffer

/**
  * Created by soap on 2018/3/27.
  */
object Collection {
  def main(args: Array[String]): Unit = {
    println("===========定长数组=========")
    /**
      * 定长数组初始化必须指定长度
      */

    val array = new Array[Int](5)
    val array1 = new Array[Int](6)
    
    println(array.mkString(","))
    println(array(1) = 1)
    println(array.mkString("a","b","c"))

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

    //    val buffToArr = buffArr.toArray

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


    println("========元组========")
    /**
      * 1.可以存放各种相同或不同类型的数据
      * 2.使用下划线下标访问具体值，下标从1开始
      * 3.使用Tuple1 或者TupleN创建元组元素必须等于N
      * Tuple1 可以传多个值，多个值将会组成一个数组
      */
    val tulpe = (1, "a", 'c', 1.4f, true)
    val tulpe1 = Tuple5(1, "a", 'c', 1.4f, true)
    println("tulpe1:" + tulpe1._1)
    println(tulpe.toString())

    println("========元组遍历1========")
    for (i <- tulpe.productIterator) {
      println(i)
    }
    println("========元组遍历2========")
    tulpe.productIterator.foreach(j => println(j))
    tulpe.productIterator.foreach(println(_))

    println("========List========")
    /**
      * 1.如果List为空则为Nil对象
      * 2.drop 安序号删除后返回新List 序号从1开始，序号可以任意值 ，没有对应序号则不删除
      */
    val list = List(1, "s", 'c', true)
    println(list.toString())
    val list2 = list :+ 1
    println(list2.toString())
    val listDrop = list.drop(-110)
    println("listDrop:" + listDrop.toString())
    println(list.toString())

    println("========List 追加========")
    /**
      *  1. :: 追加效果与 +: 一样， 在后一个集合的第一位追加前面的元素
      *   :+ 表示将后一个元素追加到前面集合的最后一位
      *
      *  2. +: 需要从后向前运算
      *  3. ++ 与 ::: 表示两个集合向连为一个新集合
      */
    val listAdd = list :+ list2
    println(listAdd.toString())

    val listAddNil1 = list ++ list2   //List(List(1,s,c,true),1,s,c,true,1)
    println("listAddNil1:"+listAddNil1)
    val listAddNil2 = list ::: list2   //List(List(1,s,c,true),1,s,c,true,1)
    println("listAddNil2:"+listAddNil2)


    val listAddNil3 = list +: list2 :: Nil   //List(List(1, s, c, true), List(1, s, c, true, 1))
    println(listAddNil3)

    val list4 = 1 :: 2 :: 3 :: list +: Nil  // List(1,2,3,List(1, s, c, true))
    println(list4)

    val list5 = 1 +: 2 +: 3 +: list  // List(1,2,3,1, s, c, true)
    println(list5)

    val list6 = Nil :+ 1 :+ 2 :+ 3 :+ list  // List(1,2,3,List(1, s, c, true))
    println(list6)    

  }

}
