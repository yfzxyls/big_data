import java.util.UUID

import PageStat.{getClass, loadUserVisitAction}
import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object ProductStat {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(getClass.getSimpleName)
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //获取过滤参数
    // val taskParam = ParamUtils.getParam("")
    val taskParams = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    // orgUserVisitAction.foreach(println(_))
    val jsonParam = JSONObject.fromObject(taskParams)

    val clickCityIdAndProduct = genProduct(sparkSession, jsonParam)

    val cityIdInfo = genCityIdAndInfo(sparkSession)

    val cityAndProductInfo = genCityAndProductInfo(sparkSession, clickCityIdAndProduct, cityIdInfo)

    val areaTop3ClickProduct = genCityAndProductFullInfo(sparkSession)

  }


  private def saveRDDToMysql(extractSessionInfo: DataFrame,
                             sparkSession: SparkSession,
                             tableName: String) = {

    extractSessionInfo.write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", tableName)
      .mode(SaveMode.Append)
      .save()
  }

  /**
    * |area|city_infos|product_id|click_count|product_name|product_status|
    * 从 tmp_city_infos_product 以及原始 product_info 获取商品 product_name
    * product_status (0:seft,1:third ) 自由商品 第三方商品
    *
    * @param sparkSession
    */
  def genCityAndProductFullInfo(sparkSession: SparkSession) = {
    //注册UDF 函数将 json 字符串指定属性提取
    sparkSession.udf.register("json_paser", (jsonStr: String, fieldName: String) => {
      val jsonObj = JSONObject.fromObject(jsonStr)
      jsonObj.getInt(fieldName)
    })
    val sql = "select cip.area,cip.city_infos,cip.product_id,click_count,product_name," +
      "if(json_paser(pi.extend_info,'product_status') = 0 , 'self', 'third') product_status " +
      "from tmp_city_infos_product cip join product_info pi " +
      "on cip.product_id = pi.product_id"
    sparkSession.sql(sql).createOrReplaceTempView("tmp_city_product_info")

    //    val sql1 = "select area,city_infos ,product_id,click_count,product_name,product_status ," +
    //      "row_number() over(partition by area order by click_count desc ) row from tmp_city_product_info"
    //    sparkSession.sql(sql1).show()
    val sql1 = "select *, CASE WHEN area='华北' OR area='华东' THEN 'A_Level' " +
      "WHEN area='华南' OR area='华中' THEN 'B_Level' " +
      "WHEN area='西北' OR area='西南' THEN 'C_Level' " +
      "ELSE 'D_Level' END area_level " +
      "from ( select area,city_infos ,product_id,click_count,product_name,product_status ," +
      "row_number() over(partition by area order by click_count desc ) row from tmp_city_product_info ) t where row<=3"
    import sparkSession.implicits._
    val taskUUID = UUID.randomUUID().toString
    val top3DS = sparkSession.sql(sql1).rdd.map(row=>{
      AreaTop3Product(taskUUID, row.getAs[String]("area"), row.getAs[String]("area_level"),
        row.getAs[Long]("product_id"), row.getAs[String]("city_infos"),
        row.getAs[Long]("click_count"), row.getAs[String]("product_name"),
        row.getAs[String]("product_status"))
    }).toDF
    saveRDDToMysql(top3DS,sparkSession,"area_click_product")


  }

  /**
    * 将城市信息与产品ID关联
    *
    * @param sparkSession
    * @param clickCityIdAndProduct
    * @param cityIdInfo
    */
  def genCityAndProductInfo(sparkSession: SparkSession,
                            clickCityIdAndProduct: RDD[(Long, Long)],
                            cityIdInfo: RDD[(Long, Row)]) = {
    //注册udf 函数 将 城市ID 与 城市名称连接
    sparkSession.udf.register("concat_long_string", (city_id: Long, city_name: String) => {
      city_id + ":" + city_name
    })
    //注册自定义udaf 函数
    sparkSession.udf.register("concat_distinct", new ConcatDistinct)
    val cityAndProductRDD = cityIdInfo.join(clickCityIdAndProduct).map {
      case (city_id, (city_info, product_id)) =>
        val cityName = city_info.getAs[String]("city_name")
        val area = city_info.getAs[String]("area")
        (city_id, cityName, area, product_id)
    }
    import sparkSession.implicits._
    //将城市信息 和 产品id 注册为 临时表
    cityAndProductRDD.toDF("city_id", "city_name", "area", "product_id")
      .createOrReplaceTempView("tmp_city_product")

    val sql = "select area, product_id,count(*) click_count," +
      "concat_distinct(concat_long_string(city_id,city_name)) city_infos from tmp_city_product group by area,product_id "
    sparkSession.sql(sql).createOrReplaceTempView("tmp_city_infos_product")
  }

  /**
    * 获取城市信息数据
    *
    * @param sparkSession
    * @return
    */
  def genCityIdAndInfo(sparkSession: SparkSession) = {
    val cityAreaInfoArray = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
      (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
      (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
      (9L, "哈尔滨", "东北"))
    import sparkSession.implicits._
    sparkSession.sparkContext.makeRDD(cityAreaInfoArray).toDF("city_id", "city_name", "area").rdd
      .map(row => (row.getAs[Long]("city_id"), row))

  }

  def genProduct(sparkSession: SparkSession, jsonParam: JSONObject) = {
    val taskJson = JSONObject.fromObject(jsonParam)
    val startDateStr = ParamUtils.getParam(taskJson, Constants.PARAM_START_DATE)
    val endDateStr = ParamUtils.getParam(taskJson, Constants.PARAM_END_DATE)
    val sql = "select city_id, click_product_id from user_visit_action where date >='" + startDateStr + "' and " +
      "date <= '" + endDateStr + "' and click_product_id is not null and click_product_id != -1"
    //    import sparkSession.implicits._
    sparkSession.sql(sql).rdd.map(row => (row.getAs[Long]("city_id"), row.getAs[Long]("click_product_id")))
  }
}
