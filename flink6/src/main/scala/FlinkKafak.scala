
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08

object PageHomeRt {

  def main(args: Array[String]): Unit = {
//
//    val groupId = "data_platform_page_home"
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    //设置跟partition数一样
//    env.setParallelism(2)
//   // val consumer = new KlKafkaConsumer08(groupId)
//   val consumer = new FlinkKafkaConsumer08("","",).setStartFromEarliest()
//
//
//
//    val data = env.addSource(consumer.getInstance(Lists.newArrayList("binlog.kuailv_kl_oms"),
//      binlogEntry).setStartFromLatest())
//    data.filter(binlogEntry => {
//      binlogEntry.getTableName.equalsIgnoreCase("biz_order") || binlogEntry.getTableName.equalsIgnoreCase("order_item")
//    }).map(binlogEntry =>{println(binlogEntry)})

  }

}

