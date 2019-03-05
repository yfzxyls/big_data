package com.soap.flink.source.datastream

/**
  * @author yangfuzhao on 2019/2/11.
  */
object EventTime {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val text = env.socketTextStream("localhost",9999).assignTimestampsAndWatermarks(new TimestampExtractor)
    text.map((_,1)).keyBy(0)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
        .maxBy(0).print()



    env.execute("event_time")
  }

  /**
    * 自定义时间戳提取器
    */
  class TimestampExtractor extends AssignerWithPeriodicWatermarks[String] with Serializable {

    override def getCurrentWatermark: Watermark = {
      new Watermark(System.currentTimeMillis())
    }

    override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
      element.split(",")(0).toLong
    }
  }


}
