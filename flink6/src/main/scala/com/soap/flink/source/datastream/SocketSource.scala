package com.soap.flink.source.datastream

/**
  * @author yangfuzhao on 2019/1/3.
  */
object SocketSource extends App with LazyLogging {

  /**
    * filter 返回false的数据将会被过滤
    *
    * @param args
    */
  override def main(args: Array[String]): Unit = {


    //    Logger.getRootLogger.setLevel(Level.WARN)
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("",1,"")

    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1,10000))

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", 9999,',',100000)

    logger.debug("=================>>>>>>>>>>>>aaaa")

    val count = text.flatMap(_.split(" "))
      .filter(!StringUtils.isEmpty(_))
      .map((_, 1))
      .keyBy(0)
      //      .timeWindow(Time.seconds(10), Time.seconds(10))
      .sum(1).setParallelism(4)
    //.reduce((v1, v2) => (v1._1, v1._2 + v2._2))

    count.print().setParallelism(1)

    env.execute("socket")
  }
}
