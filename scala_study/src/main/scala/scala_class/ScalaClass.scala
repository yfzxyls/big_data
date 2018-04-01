package scala_class

/**
  * Created by soap on 2018/3/30.
  */
object ScalaClass {

  def main(args: Array[String]): Unit = {

    /**
      * 1.类属性 var(生成getter setter) val (只生成getter)
      * 2.使用  @BeanProperty 可以生成 java 风格的getter setter
      * 3.private 修饰 生成的getter setter 也为 private
      */
    val dog = new Dog
    dog.leg_=(1)
    println(dog.toString)
    println(dog.getLeg)

    println("=========构造器=======")
    /**
      * 1主构造的参数直接放置于类名之后
      * 2.主构造器会执行类定义中的所有语句
      * 3.可通过private设置的主构造器的私有属性
      * constructor Cat in class Cat cannot be accessed in object ScalaClass
      * 4.可定义伴生类对象调用私有构造器创建对象
      * 5.重新定义构造器，构造器必须首先调用主构造器或其他构造器
      * 6.类可以访问伴生类对象的使用属性
      */

    val cat = new Cat(1, "cat")
    //伴生类对象创建对象
    //    val cat = Cat.cat("cat")
    println(cat.name)


    println("===========嵌套类===========")
    /**
      * 1.scala 默认不使用类型投影，则 netWork1 中的nick 不能添加 netWork2 中的soap
      * nick.contacts += soap   NetWork#Member
      */
    val netWork1 = new NetWork
    val netWork2 = new NetWork

    val nick = netWork1 join ("Nike")
    val thoms = netWork1 join ("Thoms")
    val soap = netWork2 join ("Soap")

    nick.contacts += thoms
    nick.contacts += nick

    nick.contacts += soap //

    println(nick.contacts.mkString(","))

    println("==========单列============")
    /**
      * 1.定义伴生对象apply方法后创建对象时可不是有new 关键字
      */
    val singletonClass1 = SingletonClass
    val singletonClass2 = SingletonClass

    println(singletonClass1 == singletonClass2)


    println("===========提前定义===========")
    /**
      * 1.子类重写父类的部分属性，如果 该属性在父类构造器中被使用，
      * 初始化时需要区子类中获取，由于子类构造器在父类构造器之后使用，
      * 所以导致该属性没有被初始化，将使用默认值
      * 2.解决以上办法可以使用 lazy 关键字
      * 3.子类使用到该属性的地方也重写
      * 4.还可以使用提前定义语法，可以在超类的构造器执行之前初始化子类的val字段  使用with 关键字
      *
      */
    val creature = new Creature
    println(creature.env.length)

    val ant = new Ant
    println(ant.env.length)

    println("=========继承==========")
    /**
      * 1.先调主构造器，后调子构造器 ,主构造器自动调用父类构造器
      * 2.子类对象可以赋值给父类
      * 3. asInstanceOf 返回实际类型对象
      * 
      */
    val man: Person = new Man()
    val person = new Person
    person.sex_=(1)

    println(man.isInstanceOf[Man])
    println(man.isInstanceOf[Person])
    println(man.asInstanceOf[Man].getClass)
    println(man.asInstanceOf[Person].getClass)
    println(classOf[Person])

    println("==========枚举=============")
    /**
      * 1.枚举以id value 存在，id为自增可以不指定，指定时不能重复
      *
      */
    println(TrafficLightColor.Red)
    println(TrafficLightColor.Red.id)

    println(TrafficLightColor.Yellow)
    println(TrafficLightColor.Yellow.id)

    println(TrafficLightColor.Green)
    println(TrafficLightColor.Green.id)

//    println(TrafficLightColor.Go)
//    println(TrafficLightColor.Go.id)

  }


}
