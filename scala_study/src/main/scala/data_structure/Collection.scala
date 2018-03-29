package data_structure

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by soap on 2018/3/27.
  */
object Collection {
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
    val array2 = Array(12,8,5,4,5)
    println(array2.sum)
    val res0 = array2.sortWith(_ < _)
    println(res0.mkString(","))
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
      * :+ 表示将后一个元素追加到前面集合的最后一位
      *
      *  2. +: 需要从后向前运算
      *  3. ++ 与 ::: 表示两个集合向连为一个新集合
      */
    val listAdd = list :+ list2
    println(listAdd.toString())

    val listAddNil1 = list ++ list2 //List(List(1,s,c,true),1,s,c,true,1)
    println("listAddNil1:" + listAddNil1)
    val listAddNil2 = list ::: list2 //List(List(1,s,c,true),1,s,c,true,1)
    println("listAddNil2:" + listAddNil2)


    val listAddNil3 = list +: list2 :: Nil //List(List(1, s, c, true), List(1, s, c, true, 1))
    println(listAddNil3)

    val list4 = 1 :: 2 :: 3 :: list +: Nil // List(1,2,3,List(1, s, c, true))
    println(list4)

    val list5 = 1 +: 2 +: 3 +: list // List(1,2,3,1, s, c, true)
    println(list5)

    val list6 = Nil :+ 1 :+ 2 :+ 3 :+ list // List(1,2,3,List(1, s, c, true))
    println(list6)

    println("============Queue 队列=============")
    /**
      * 1.队列创建必须指定范型
      * 2.队列可以直接追加值
      * 3.值可重复，先进先出
      */
    val q = new mutable.Queue[Any]
    q += 1
    q += 1.2
    //追加List
    q ++= List(1, 3, 5)
    println(q)
    q.dequeue()
    q.enqueue(1)
    println(q)

    println(q.head)
    println(q.tail)
    println(q.front)
    println(q.last)

    println("=============Map ===============")
    /**
      * 1.可变集合初始化后才能添加元素,模式匹配
      * 2.key相同重新赋值将 覆盖原有值
      * 3.建议使用map.get(key) 获取值，存在则返回，不存在则返回None对象；
      * 4.可以使用 ++ 对map进行连接，形成新的map;可变map 可使用++= 扩展原map
      * 5.使用for循环遍历map 可以使用（k,v） (a) 接受key value 或者健值对
      *
      */
    //不可变
    val map1 = Map("Alice" -> "ss", "Bob" -> 14, "Mike" -> 4)
    map1.drop(0)
    println(map1)

    val map2 = mutable.Map("Alice" -> 13, "Bob" -> 14, "Mike" -> 4)
    map2 += ("Alice" -> 15)
    for (x <- map2) {
      println(x)
    }

    println(map2("Alice"))
    //    println(map2("Alisce"))//key not found: Alisce
    println(map2.get("Alisce"))
    val map3 = mutable.Map("a" -> 1, "s" -> 3)
    val mapAddMap = map3 ++= map2
    println(mapAddMap.mkString(","))
    println(map3.mkString(","))

    println("==============遍历=============")
    for ((k, v) <- mapAddMap) {
      println(k + "=" + v)
    }
    for ((k) <- mapAddMap) {
      println(k)
    }

    for ((v) <- mapAddMap.values) {
      println(v)
    }

    println("============== Set 集合==============")

    /**
      * 1.Set 默认使用hash值进行排序
      * 2.使用构造器传递set创建的set为不可变集合
      */
    val set = Set(1, "w", 's', true)
    val addSet = set.+(2)
    println(set)
    println(addSet)

    val set1 = mutable.Set(1, 2, "s")
    val set2 = mutable.Set(set1)
    //    set2 += 3 //不能进行
    set1.add(5)
    //    set1 += set   //可以有
    set1 += "103"
    println(set1.mkString(","))
    set1 -= 2 //删除指定元素
    set1 -= "s" //删除某一元素
    set1.remove(2)
    println(set1.mkString(","))

    for (x <- set1) {
      println(x)
    }

    println("=======集合运算==============")
    /**
      * 1.可变集合 & 交集
      * 2.&~ 差运算 ，返回前一集合存在，后一集合不存在的值
      * 3.drop 返回集合中出去前 n 个元素后的新集合
      */

    val set3 = mutable.Set(1, 2, 3)
    val set4 = mutable.Set(2, 3, 4, 5)
    val res1 = set3.&(set4)
    val res2 = set3.&~(set4)
    val res3 = set4.&~(set3)
    println(res1)
    println(res2)
    println(res3)

    //    println(set4.drop(1))
    val x = 2
    //TODO:   _.equals(2)???  _.equals(5) 返回去掉5的值
    val setDropWhile = set4.dropWhile(_.equals(4))
    println(setDropWhile)
    println(set4)
    println(set4.min)
    println(set4.max)

    println("===========集合映射为函数")
    var i = 0
    val setMap = set4.map(x => {
      i += 1
      i
    })
    println(setMap.toString())

    println("=======化简、折叠、扫描===========")
    /**
      * 1.reduceLeft(_+_) =   reduce(_+_) == sum
      * 2.reduceLeft 从左到右，运算后的值放在左变继续与下一元素运算
      * 3.reduceRight 从右到左，运算后的值放在右变继续与下一元素运算
      * 4.对于Set集合，按添加元素入顺序计算
      */
    val set5 = mutable.Set(1, 2, -3, 4, 5)
    val set6 = mutable.Set("1", "2", "-3", "4", "5")
    val res4 = set5.sum //  reduceLeft(_+_) =   reduce(_+_) == sum
    println("set5:" + set5)
    val res5 = set5.reduceLeft(_ - _) //
    println("res5" + res5) //=-3-1-2-5-4 =15
    val res6 = set5.reduceRight(_ - _) //-3 1 5 2 4
    //TODO:顺序
    println("res6:" + res6) // 1-2-(-3-(5-4))3 ???  -3, 1, 5, 2, 4
    println(set6)
    val res7 = set6.reduceRight(_ + _) //
    val res8 = set6.reduceLeft(_ + _) //
    println(res4)

    println(res7)
    println(res8)


    println("=======折叠，化简：fold==========")
    /**
      * 1.简化: 中间值放在\ 或 / 一侧
      * list51.foldLeft(100)(_ - _) -----> (100/:list51(_-_))
      * list51.foldRight(100)(_ - _) ----> (list51:\100)(_-_)
      */
    val list3 = List(1, 9, 2, 8)
    val i4 = list3.fold(5)((sum, y) => sum + y)
    println(i4)

    val list51 = List(1, 9, 2, 8)
    println(list51)
    val i5 = list51.foldLeft(100)(_ - _) // 1--91
    println(i5)
    val i6 = list51.foldRight(100)(_ - _) // 1-9+94)    95-9 =86
    println(i6)

    val res9 = (list51 :\ 100) (_ - _)
    println(res9)


    println("=============统计字符出现次数=============")
    /**
      * 第一次 k = Map() ,v = “一”
      * 从k里获取“一 ”没有 则将 （"一 "-> 1） 放入Map
      * 第二次 k=Map("一 "-> 1) ,v = "首" ，Map中没有 则将 v=“首” 放入Map
      *
      */
    val sentence = "一首现代诗《笑里藏刀》:哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈刀哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈哈"
    val res10 = (mutable.Map[Char, Int]() /: sentence) ((k, v) => {
      //      println(k)
      k + (v -> (k.getOrElse(v, 0) + 1))
    })
    println(res10)


    println("============拉链操作=============")
    /**
      * 1.将两个一元集合组合成二元集合，二元组合为三元 ......
      *
      */
    val listL1 = List("15837312345", "13737312345", "13811332299")
    val listL2 = List("张扬", "狂妄")

    val res11 = listL1 zip listL2 zip listL2
    println(res11)


    println("========迭代器========")
    val iterator = List(1, 2, 3, 4, 5).iterator
    //    while (iterator.hasNext) {
    //      println(iterator.next())
    //    }
    //
    //    for (enum <- iterator) {
    //      println(enum)
    //    }
    println("======== Stream 流========")

    /**
      * 1.使用#::得到一个stream
      * 2.切勿使用.last .toArray 等获取操作 会造成死循环
      */
    def numsForm(n: BigInt): Stream[BigInt] = n #:: numsForm(n + 1)

    val stream = numsForm(2)
    println("*" + stream.map(x => x * x))

    println(stream)
    println(stream.tail)
    println(stream)
    println(stream.tail)
    println(stream)
    println(stream.tail)
    println(stream)

    println(stream.map(x => x * x))


    println("===视图 View==========")
    val view = (1L to 100).view.map(x => x).filter(y => y.toString == y.toString.reverse)
    println(view.mkString(" "))
    for (x <- view) {
      print(x + "，")
    }

    println("==========并行集合===========")
    (1 to 5).foreach(print(_))
    println("\n----------------------")
    (1 to 5).par.foreach(print(_))

  }


}
