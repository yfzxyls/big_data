package scala_implicit

import java.io.File

/**
  * Created by soap on 2018/3/31.
  */
object ScalaImplicit {

  def main(args: Array[String]): Unit = {

    /**
      * 1.隐士转换会搜索当前类中是否有可用的转换机制
      */

    implicit def a(d: Double) = d.toInt

    //不加上边这句你试试
    val i1: Int = 3.5
    println(i1)

    println("========隐式转换扩展类==========")

    /**
      * 1.隐式类转换方法， 加 implicit  输入原类型，返回扩展类型
      */

    implicit def file2RichFile(from: File) = {
      println("file ---> RichFile")
      new RichFile(from)
    }

    val contents = new File("D:\\study\\big_data\\scala_study\\scala_study.iml").read
    println(contents)
    println("========隐式值=======")
    /**
      *  1.隐式值不能有二意性
      */
    //    implicit val name2 = "Nick"
    implicit val myName = "Nick"

    def person(implicit name: String) = name

    println(person)

    println("=========隐式转换调用类中本不存在的方法==========")

    import scala_implicit.Learn.learningType
    val dog = new Dog
    dog.fly(dog, "飞行技能")

    println("==========隐式类=========")
    /**
      * 隐式类可以对java中的类进行扩展，不需要实现java类
      * 1.其所带的构造参数有且只能有一个
      * 2.隐式类必须被定义在“类”或“伴生对象”或“包对象”里
      * 3.隐式类不能是case class（case class在定义会自动生成伴生对象与2矛盾）
      * 4.作用域内不能有与之相同名称的标示符
      * 5.调用时必须导入隐式类扩展的方法
      */
    import scala_implicit.StringUtils._
    println("mobin".increment)

    println("文件和正则表达式 ")
    println("=========读取行====")

    import scala.io.Source
    val file1 = Source.fromFile("D:\\study\\big_data\\scala_study\\info.txt")
    val lines = file1.getLines
    for (line <- lines) {
      println(line)
    }
    file1.close

    println("=========文件内容转数组：======")
    /**
      * 文件流中的数据只能读取一次
      */
    val file2 = Source.fromFile("D:\\study\\big_data\\scala_study\\info.txt")
    val res0 = file2.mkString
    println(res0.toString)
//    val lines2 = file2.getLines.toArray
//    println(lines2.mkString("|"))
    file2.close()

    println("=======将文件按照指定符号分割读取为字符串数组==========")

    val file3 = Source.fromFile("D:\\study\\big_data\\scala_study\\info.txt")
    val lines3 = file3.getLines().mkString(",").split(",")
    println(lines3.mkString("|"))
    file3.close()

    println("========读取字符========")
    val file4 = Source.fromFile("D:\\study\\big_data\\scala_study\\info.txt", "UTF-8")
    for(ch <- file4 if !ch.equals("\n\t")){
      println(ch)
    }
    file4.close

    println("===========正则===========")
    import scala.util.matching.Regex
    val pattern1 = new Regex("(S|s)cala")
    val pattern2 = "(S|s)cala".r
    val str = "Scala is scalable and cool"
    println((pattern2 findAllIn str).mkString(","))
    
  }
}
