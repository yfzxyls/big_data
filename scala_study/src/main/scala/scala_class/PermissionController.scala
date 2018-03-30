package scala_class

/**
  * Created by soap on 2018/3/30.
  */


package society {
  package professional {

    class Executive {
      private[professional] var workDetails = null
      private[society] var friends = null
      private[this] var secrets = null  // secrets 属于本对象，其他本类的对象无法访问

      def help(another: Executive) {
        println(another.workDetails)
//                println(another.secrets) //报错：访问不到
      }
    }

  }

}

