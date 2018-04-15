package com.soap.session

import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.collection.mutable

object UserVisitSessionAnalyze {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName(getClass.getSimpleName)
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //获取过滤参数
    //    val taskParam = ParamUtils.getParam("")
    val taskParams = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val orgUserVisitAction = loadUserVisitAction(sparkSession, taskParams)
    //        orgUserVisitAction.foreach(println(_))
    val sessionIdAndUserVisitAction = transformSessionIdAndUserVisitActionMap(orgUserVisitAction)
    val fullInfo = transformFullInfo(sparkSession, sessionIdAndUserVisitAction)
    //注册自定义累加器
    val sessionStatisticAccumulator = new SessionStatisticAccumulator
    sparkSession.sparkContext.register(sessionStatisticAccumulator, "SessionStatisticAccumulator")
    val filterInfo = filterAndAcc(fullInfo, taskParams, sessionStatisticAccumulator)
    //执行action 操作
    filterInfo.count()
    //filterInfo.foreach(println(_))

    // 生成全局唯一的主键
    val taskUUID = UUID.randomUUID().toString
    getSessionPercentStatistic(sparkSession, taskUUID, sessionStatisticAccumulator.accMap)

  }

  def getSessionPercentStatistic(sparkSession: SparkSession,
                                 taskUUID: String,
                                 value: mutable.HashMap[String, Int]): Unit = {
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val aggRdd = sparkSession.sparkContext.makeRDD(Array(stat))

    import sparkSession.implicits._
    aggRdd.toDF().write.format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_aggr_stat")
      .mode(SaveMode.Append)
      .save()

  }

  /**
    * 对参数进行过滤并执行累加操作
    *
    * @param fullInfo
    * @param taskParams
    */
  def filterAndAcc(fullInfo: RDD[(String, String)],
                   taskParams: String,
                   sessionStatisticAccumulator: SessionStatisticAccumulator) = {

    val taskJson = JSONObject.fromObject(taskParams)
    val startAge = ParamUtils.getParam(taskJson, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskJson, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskJson, Constants.PARAM_PROFESSIONALS)
    val sex = ParamUtils.getParam(taskJson, Constants.PARAM_SEX)
    val cities = ParamUtils.getParam(taskJson, Constants.PARAM_CITIES)
    val keywords = ParamUtils.getParam(taskJson, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskJson, Constants.PARAM_CATEGORY_IDS)
    val targetPageFlow = ParamUtils.getParam(taskJson, Constants.PARAM_TARGET_PAGE_FLOW)

    //startAge=20|endAge=25
    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if (filterInfo.endsWith("\\|")) {
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    }

    val sessionId2FilteredRDD = fullInfo.filter { case (sessionId, fullInfo) => {
      var success = true
      if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, startAge, endAge)) success = false
      if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) success = false
      if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) success = false
      if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) success = false
      if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) success = false
      if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS)) success = false
      if (success) {
        //累加session访问次数
        sessionStatisticAccumulator.add(Constants.SESSION_COUNT)

        /**
          * 根据 session 访问时间段累加
          *
          * @param visitLength
          */
        def calculateVisitLength(visitLength: Long): Unit = {
          if (visitLength >= 1 && visitLength <= 3) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
          } else if (visitLength >= 4 && visitLength <= 6) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
          } else if (visitLength >= 7 && visitLength <= 9) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
          } else if (visitLength >= 10 && visitLength <= 30) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
          } else if (visitLength > 30 && visitLength <= 60) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
          } else if (visitLength > 60 && visitLength <= 180) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
          } else if (visitLength > 180 && visitLength <= 600) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
          } else if (visitLength > 600 && visitLength <= 1800) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
          } else if (visitLength > 1800) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
          }
        }

        /**
          * 根据 访问步长区间 进行累加
          *
          * @param stepLength
          */
        def calculateStepLength(stepLength: Long): Unit = {
          if (stepLength >= 1 && stepLength <= 3) {
            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
          } else if (stepLength >= 4 && stepLength <= 6) {
            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
          } else if (stepLength >= 7 && stepLength <= 9) {
            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
          } else if (stepLength >= 10 && stepLength <= 30) {
            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
          } else if (stepLength > 30 && stepLength <= 60) {
            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
          } else if (stepLength > 60) {
            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
          }
        }

        val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
        val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
        calculateVisitLength(visitLength)
        calculateStepLength(stepLength)
      }
      success
    }
    }
    sessionId2FilteredRDD
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
    sparkSession.sql(sql).as[UserVisitAction].rdd.cache()
  }

  def transformFullInfo(sparkSession: SparkSession, sessionIdAndUserVisitAction: RDD[(Long, String)]) = {

    val sql = "select * from user_info"
    import sparkSession.implicits._
    val userId2UserInfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))
    val sessionId2FullInfoRDD = sessionIdAndUserVisitAction.join(userId2UserInfoRDD).map {
      case (userId, (aggrInfo, userInfo)) => {
        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        val fullInfo = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city
        (sessionId, fullInfo)
      }
    }
    sessionId2FullInfoRDD
  }


  def transformSessionIdAndUserVisitActionMap(orgUserVisitAction: RDD[UserVisitAction]) = {
    orgUserVisitAction.map(item => (item.session_id, item)).groupByKey().map { case (k, v) => {
      var startTime: Date = null
      var endTime: Date = null
      val searchKeywords = new StringBuffer()
      val cilckCategorys = new StringBuffer()
      //访问时长 session 时长
      var visitLength = 0L
      var stepLength = 0L

      var userId = -1L
      for (userVisit <- v) {
        if (userId == -1) {
          userId = userVisit.user_id
        }
        val actionDate = DateUtils.parseTime(userVisit.action_time)
        if (startTime == null || startTime.after(actionDate)) {
          startTime = actionDate
        }
        if (endTime == null || endTime.before(actionDate)) {
          endTime = actionDate
        }
        if (!StringUtils.isEmpty(userVisit.search_keyword) && !searchKeywords.toString.contains(userVisit.search_keyword)) {
          searchKeywords.append(userVisit.search_keyword + ",")
        }
        if (userVisit.click_category_id != -1L && !cilckCategorys.toString.contains(userVisit.click_category_id)) {
          cilckCategorys.append(userVisit.click_category_id + ",")
        }
        stepLength += 1
      }

      val searchKw = StringUtils.trimComma(searchKeywords.toString)
      val clickCg = StringUtils.trimComma(cilckCategorys.toString)

      visitLength = (endTime.getTime - startTime.getTime) / 1000

      val aggrInfo = Constants.FIELD_SESSION_ID + "=" + k + "|" +
        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
        Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
        Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

      (userId, aggrInfo)
    }
    }
  }
}
