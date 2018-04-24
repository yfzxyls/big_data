import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

object SparkHBase {

  def main(args: Array[String]): Unit = {
    //创建HBse 配置对象
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "data1")

    //使用 ConnectionFactory 创建 table
    val table = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf("data1"))

    for (i <- 1 to 5) {
      val put = new Put(Bytes.toBytes("row" + i))
      put.addColumn(Bytes.toBytes("v"),
        Bytes.toBytes("value"),
        Bytes.toBytes("value" + i))
      table.put(put)
    }
  }
}
