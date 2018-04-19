import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * 自定义弱类型 UDAF 函数 ，将输入字段去重后以逗号分割连接
  */
class ConcatDistinct extends UserDefinedAggregateFunction {

  /**
    * 输入类型
    *
    * @return
    */
  override def inputSchema: StructType = StructType(StructField("nameInfo", StringType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("nameInfo", StringType) :: Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!buffer.getString(0).contains(input.getString(0))) {
      if ("".equals(buffer(0))) {
        buffer(0) = input.getString(0)
      } else {
        buffer(0) += "," + input.getString(0)
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    for (input <- buffer2.getString(0).split(",")) {
      if (!buffer1.getString(0).contains(input)) {
        if ("".equals(buffer1(0))) {
          buffer1(0) = input
        } else {
          buffer1(0) += "," + input
        }
      }
    }
  }

  override def evaluate(buffer: Row): Any = buffer.getString(0)
}

object ConcatDistinct {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ConcatDistinct").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //注册自定义UDAF函数
    sparkSession.udf.register("concatDistinct", new ConcatDistinct())
    val person = List(("Nick", 22), ("Make", 21), ("Make", 24), ("Thoms", 23))
    import sparkSession.implicits._
    val personDF = sparkSession.sparkContext.makeRDD(person).toDF("name", "age")
    personDF.createTempView("person")
    sparkSession.sql("select concatDistinct(name) from person")
      .collect().foreach(println(_))
  }
}
