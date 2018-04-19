import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, NumberUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.collection.mutable

object PageStat {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(getClass.getSimpleName)
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //获取过滤参数
    // val taskParam = ParamUtils.getParam("")
    val taskParams = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val orgUserVisitAction = loadUserVisitAction(sparkSession, taskParams).cache()
    // orgUserVisitAction.foreach(println(_))
    val jsonParam = JSONObject.fromObject(taskParams)
    val targetPageFlow = ParamUtils.getParam(jsonParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",")
    //将过滤条件转换为1_2,2_3,3_4 ....
    val targetPage = targetPageFlow.init.zip(targetPageFlow.tail).map(item => item._1 + "_" + item._2)


    val actionPageFolw = genActionPageFlow(orgUserVisitAction, targetPage,sparkSession)

  }

  /**
    * 获取每一个session的页面访问流(session,infoAction)
    * 1. 按照action_time对session所有的行为数据进行排序
    * 2. 通过map操作得到action数据里面的page_id
    * 3. 得到按时间排列的page_id之后，先转化为页面切片形式
    * 4. 过滤，将不存在于目标统计页面切片的数据过滤掉
    * 5. 转化格式为(page1_page2, 1L)
    *
    * @param orgUserVisitAction
    * @param targetPage
    */
  def genActionPageFlow(orgUserVisitAction: RDD[UserVisitAction],
                        targetPage: Array[String],
                        sparkSession:SparkSession) = {
    val page_ids = orgUserVisitAction.map(item => (item.session_id, item)).groupByKey().flatMap {
      case (_, userVisitActions) =>
        val pageId = userVisitActions.toList.sortWith((visitAction1, visitAction2) =>
          DateUtils.parseTime(visitAction1.action_time).getTime < DateUtils.parseTime(visitAction2.action_time).getTime
        ).map(_.page_id)
        //使用zip将pageId 转化为下划线形式
        val page_id = pageId.init.zip(pageId.tail)
        page_id.map { case (page1, page2) => page1 + "_" + page2 }.filter(targetPage.contains(_))
    }
    val pageStatCount = page_ids.map((_, 1D)).countByKey()
    //获取起始页的点击次数
    val startPage = targetPage(0).split("_")(0)
    var startPageCount = orgUserVisitAction.filter(_.page_id == startPage.toLong).count().toDouble
    val pageStatCountMap = new mutable.HashMap[String, Double]()
    for ((pageStat, count) <- pageStatCount) {
      val pageStatRate = NumberUtils.formatDouble( count/ startPageCount, 2)
      pageStatCountMap.put(pageStat,pageStatRate)
      startPageCount = count
    }
    var rateStr = ""
    pageStatCountMap.foreach(item=> rateStr += item._1 + "=" + item._2 + "|")
    val pageSplitConvertRate = new PageSplitConvertRate(UUID.randomUUID().toString,rateStr)
    import sparkSession.implicits._
    val pageSplitConvertRateDF = sparkSession.sparkContext.makeRDD(Array(pageSplitConvertRate)).toDF
    pageSplitConvertRateDF.write.format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "page_split_convert_rate")
      .mode(SaveMode.Append)
      .save()
  }

  def loadUserVisitAction(sparkSession: SparkSession, taskParams: String) = {
    val taskJson = JSONObject.fromObject(taskParams)
    val startDateStr = ParamUtils.getParam(taskJson, Constants.PARAM_START_DATE)
    val endDateStr = ParamUtils.getParam(taskJson, Constants.PARAM_END_DATE)

    val sql = "SELECT * FROM " + Constants.TABLE_USER_VISIT_ACTION + " WHERE date >= '" +
      startDateStr + "' AND date <=  '" + endDateStr + "'"
    //必须加隐式转换，否则没有as 方法
    import sparkSession.implicits._
    //加载数据并缓存
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }
}
