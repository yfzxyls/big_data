package example_class

/**
  * Created by soap on 2018/3/28.
  * 样例类学习
  */
object ExampleClass {

  def main(args: Array[String]): Unit = {

    /**
      * 1.先根据类型匹配，匹配对象没有参数，则只匹配类型，否则调用equals
      */
    println("==========样例类 ================")
    for (amt <- Array(Dollar(1000.0), Currency(1000.0, "EUR"), Nothing)) {
      val result = amt match {
        case Dollar(1000) => "$"
        case Currency(_, u) => u
        case Nothing => ""
        case _ => "default"
      }
      println(amt + ": " + result)
      //Dollar(1000.0): $1000.0
      //Currency(1000.0, "EUR"): EUR
      //Nothing:
    }

    println("==========样例类 赋值 ================")
    /**
      * 1.可以使用copy对样例类属性进行修改赋值
      * 2.可以不填写属性名称，参数类型需要跟声明时一致，可以少参数但默认会从前面参数进行匹配
      */
    val amt = Currency(29.95, "EUR")
    //    val price = amt.copy(19.95)
    val price = amt.copy(19.95)
    amt.copy(19.95, "RMB")
    //    amt.copy("",1)  不可以
    //    amt.copy("")  不可以
    println(amt)
    println(price)
    println(amt.copy(unit = "CHF"))

    println("=======匹配嵌套结构==========")
    val sale = Bundle("愚人节大甩卖系列", 10,
      Article("《九阴真经》", 40),
      Bundle("从出门一条狗到装备全发光的修炼之路系列", 20,
        Article("《如何快速捡起地上的装备》", 80),
        Article("《名字起得太长躲在树后容易被地方发现》", 30)))

    val result1 = sale match {
      case Bundle(_, _, Article(descr, _), _*) => descr //《九阴真经》
    }
    println(result1)

    println("=========通过@表示法将嵌套的值绑定到变量。_*绑定剩余Item到rest")
    val result2 = sale match {
      case Bundle(_, _, art@Article(_, _), rest@_*) => (art, rest)
    }
    println(result2)
    println("======不使用_*绑定剩余Item到rest=========")
    val result3 = sale match {
      case Bundle(_, _, art@Article(_, _), rest) => (art, rest)
    }
    println(result3)
    /*
        val sale = Bundle("愚人节大甩卖系列", 10,
          Article("《九阴真经》", 40),
          Bundle("从出门一条狗到装备全发光的修炼之路系列", 20,
            Article("《如何快速捡起地上的装备》", 80),
            Article("《名字起得太长躲在树后容易被地方发现》", 30)))*/
    println("====计算某个Item价格的函数，并调用========")

    def price1(it: Item): Double = {
      it match {
        case Article(_, p) => p
        case Bundle(_, disc, its@_ *) => {
          its.map(price1 _).sum - disc
        }
      }
    }

    println(price1(sale))

    println("====模拟枚举========")
    for (color <- Array(Red, Yellow, Green))
      println(
        color match {
          case Red => "stop"
          case Yellow => "slowly"
          case Green => "go"
        })

    println("============偏函数 ===========")
    /**
      * 1.偏函数在匹配时会先调用 isDefinedAt 判断是否有匹配项
      */
    val f: PartialFunction[Char, Int] = {
      case '+' => 1
      case '-' => -1
    }
    println(f('-'))
    println(f.isDefinedAt('0'))
    println(f('+'))
    if (f.isDefinedAt('*')) {
      println(f('*'))
    }

    println("============偏函数 应用 ===========")
    /**
      * 1.集合  collect 方法需要一个偏函数，会自动调用isDefinedAt 处理 推荐使用
      * 2.map方法需要普通函数
      */
    //    val f1 = new PartialFunction[Any, Int] {
    //      def apply(any: Any) = any.asInstanceOf[Int] + 1
    //
    //      def isDefinedAt(any: Any) = if (any.isInstanceOf[Int]) true else false
    //    }
    //    val rf1 = List(1, 3, 5, "seven") map f1
    //    println(rf1)

    val list = List(1, 2, 3, 4, "HelloWorld", 6)

    val res1 = list.map(x => x + "")
//    val res2 = list.map({ case i: Int => i + 1 }) //类型匹配异常
    println(res1)
    val res2 = list.collect({ case i: Int => i + 1 })
    println(res2)
  }
}
