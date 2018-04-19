package com.soap.session

import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.collection.mutable
import scala.util.Random

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

    //生成全局唯一的主键
    val taskUUID = UUID.randomUUID().toString
    //将累加器中的数据保存至数据库
    //getSessionPercentStatistic(sparkSession, taskUUID, sessionStatisticAccumulator.accMap)

    //getExtractSession(sparkSession, filterInfo, taskUUID)

    val sessionIdAndVisitAction = orgUserVisitAction.map(item => (item.session_id, item))
    val filterSessionIdAndVisitAction = filterInfo.join(sessionIdAndVisitAction).map(item => (item._1, item._2._2))

    val top10Category = getTopNCategory(filterSessionIdAndVisitAction, sparkSession, taskUUID, 10)


    val top10ClickCategory = getTop10ClickCategory(top10Category, sessionIdAndVisitAction, sparkSession,taskUUID)
  }


  /**
    * 点击类别TOP10的session的TOP10
    * @param top10Category
    * @param sessionIdAndVisitAction
    * @param sparkSession
    * @param taskId
    */
  def getTop10ClickCategory(top10Category: Array[(Category, Long)],
                            sessionIdAndVisitAction: RDD[(String, UserVisitAction)],
                            sparkSession: SparkSession,
                            taskId: String) = {
    val clickCategoryId = sessionIdAndVisitAction.map {
      case (sessionId, userVisitAction) =>
        (userVisitAction.click_category_id, userVisitAction)
    }
    val top10CategoryId = sparkSession.sparkContext.makeRDD(top10Category).map(item => (item._2, 1))

    // cid Array(userVisitAction)
    val top10SessionCategory = top10CategoryId.join(clickCategoryId).map(item => (item._2._2.session_id, item._2._2)).groupByKey()

    val top10CategoryIdAndSession = top10SessionCategory.flatMap {
      case (sessionId, userVisitActions) =>
        //同一session 中 点击次数
        val clickCategoryCount = new mutable.HashMap[Long, Long]()
        for (action <- userVisitActions) {
          if (!clickCategoryCount.contains(action.click_category_id))
            clickCategoryCount += action.click_category_id -> 0
          clickCategoryCount.update(action.click_category_id, clickCategoryCount(action.click_category_id) + 1)
        }
        for ((cid, count) <- clickCategoryCount) yield (cid, sessionId + "=" + count)
    }.groupByKey()

    val top10Session = top10CategoryIdAndSession.flatMap {
      case (cid, sessionCount) =>
        val top10Session = sessionCount.toList.sortWith((sessionCount1, sessionCount2) =>
          sessionCount1.split("=")(1) > sessionCount2.split("=")(1)
        ).take(10)
        top10Session.map {
          case (topSessionCount) =>
            val sessionId = topSessionCount.split("=")(0)
            val count = topSessionCount.split("=")(1).toLong
            new Top10Session(taskId, cid, sessionId, count)
        }
    }
    import sparkSession.implicits._
    saveRDDToMysql(top10Session.toDF,sparkSession,"top_click_session")

  }

  def getTopNCategory(filterInfo: RDD[(String, UserVisitAction)],
                      sparkSession: SparkSession,
                      taskUUID: String,
                      topN: Int
                     ): Array[(Category, Long)] = {
    //过滤出点击 下单 付款的 categoryid
    val categoryIds = filterInfo.flatMap {
      case (sessionId, sessionInfo) =>
        val categoryIds = new mutable.HashMap[Long, Long]()
        if (sessionInfo.click_category_id != -1 && !categoryIds.contains(sessionInfo.click_category_id)) {
          categoryIds.put(sessionInfo.click_category_id, 1L)
        } else if (sessionInfo.order_product_ids != null) {
          for (orderCategoryId <- sessionInfo.order_product_ids.split("_")) {
            if (!categoryIds.contains(orderCategoryId.toLong)) {
              categoryIds.put(orderCategoryId.toLong, 1L)
            }
          }
        } else if (sessionInfo.pay_product_ids != null) {
          for (payCategoryId <- sessionInfo.pay_product_ids.split("_")) {
            if (!categoryIds.contains(payCategoryId.toLong)) {
              categoryIds.put(payCategoryId.toLong, 1L)
            }
          }
        }
        categoryIds
    }.distinct()


    val clickCategoryIds = getClickCategoryIdCount(filterInfo)
    val orderCategoryIds = getOrderCategoryIdCount(filterInfo)
    val payCategoryIds = getPayCategoryIdCount(filterInfo)

    val fullCategoryIds = getFullCategoryIds(categoryIds, clickCategoryIds, orderCategoryIds, payCategoryIds)
    val topNCategorySort = fullCategoryIds.map {
      case (categoryId, fullInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong
        (new Category(clickCount, orderCount, payCount), categoryId)
    }.sortByKey(false)
    val topNCategory = topNCategorySort.take(topN)
    val topNCategoryRdd = topNCategory.map {
      case (category, categoryId) => new Top10Category(taskUUID, categoryId, category.click, category.order, category.pay)
    }
    import sparkSession.implicits._
    saveRDDToMysql(sparkSession.sparkContext.makeRDD(topNCategoryRdd).toDF, sparkSession, "top10_category")
    topNCategory
  }

  /**
    * 获取点击的 CategoryId
    *
    * @param filterInfo
    * @return
    */
  def getClickCategoryIdCount(filterInfo: RDD[(String, UserVisitAction)]) = {
    filterInfo.filter(item => item._2.click_category_id != -1).map(action => (action._2.click_category_id, 1L))
      .reduceByKey(_ + _)
  }

  /**
    * 获取下单的 CategoryId
    *
    * @param filterInfo
    * @return
    */
  def getOrderCategoryIdCount(filterInfo: RDD[(String, UserVisitAction)]) = {
    filterInfo.filter(item => item._2.order_category_ids != null).flatMap {
      case (sessionId, userVisitAction) => userVisitAction.order_category_ids.split(",").map(item => (item.toLong, 1L))
    }.reduceByKey(_ + _)
  }

  /**
    * 获取下单的 CategoryId
    *
    * @param filterInfo
    * @return
    */
  def getPayCategoryIdCount(filterInfo: RDD[(String, UserVisitAction)]) = {
    filterInfo.filter(item => item._2.pay_category_ids != null).flatMap {
      case (sessionId, userVisitAction) => userVisitAction.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }.reduceByKey(_ + _)
  }

  def getFullCategoryIds(categoryIds: RDD[(Long, Long)],
                         clickCategoryIds: RDD[(Long, Long)],
                         orderCategoryIds: RDD[(Long, Long)],
                         payCategoryIds: RDD[(Long, Long)]) = {
    val clickCategory = categoryIds.leftOuterJoin(clickCategoryIds).map {
      case (categoryId, (cid, categoryCount)) =>
        val count = if (categoryCount.isDefined) categoryCount.get else 0
        (categoryId, Constants.FIELD_CLICK_COUNT + "=" + count)
    }

    val orderCategory = clickCategory.leftOuterJoin(orderCategoryIds).map {
      case (categoryId, (clickInfo, orderCount)) =>
        val count = if (orderCount.isDefined) orderCount.get else 0
        (categoryId, clickInfo + "|"
          + Constants.FIELD_ORDER_COUNT + "=" + count)
    }
    val payCategory = orderCategory.leftOuterJoin(payCategoryIds).map {
      case (categoryId, (orderInfo, payCount)) =>
        val count = if (payCount.isDefined) payCount.get else 0
        (categoryId, orderInfo + "|"
          + Constants.FIELD_PAY_COUNT + "=" + count)
    }
    payCategory
  }

  def getExtractSession(sparkSession: SparkSession,
                        filterInfo: RDD[(String, String)],
                        taskUUID: String) = {
    //将数据转化为 date_HH info 结构
    val dateHourInfo = filterInfo.map {
      case (sessionId, info) => {
        val dateTime = StringUtils.getFieldFromConcatString(info, "\\|", Constants.FIELD_START_TIME)
        val dateHour = DateUtils.getDateHour(dateTime)
        //转化为 2018-04-01_12 info
        (dateHour, info)
      }
    }

    //获取每小时 session 条数
    val countHour = dateHourInfo.countByKey()

    //每小时 session 条数
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()
    //date ,(hour,count)
    for ((dateHour, count) <- countHour) {
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)
      dateHourCountMap.get(date) match {
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour -> count)
        case Some(v) => v += (hour -> count)
      }
    }

    //计算每天应该抽取数量
    val perDateSession = 100 / dateHourCountMap.size


    val random = new Random()


    /**
      * 根据当天某小时的占比生成该小时的索引
      *
      * @param perSession
      * @param hourCountMap
      * @param hourCountIndexMap
      */
    def getEtractRandomIndex(perSession: Long,
                             hourCountMap: mutable.HashMap[String, Long],
                             hourCountIndexMap: mutable.HashMap[String, mutable.Set[Long]]) = {
      for ((hour, count) <- hourCountMap) {
        //根据 每天某一小时的访问占比，计算 该小时应该抽取的数量
        var perHourCount = ((count / perSession.toDouble) * perDateSession).toInt
        //TODO:可以不修正
        //if (perHourCount > count)perHourCount = count.toInt
        hourCountIndexMap.get(hour) match {
          case None => hourCountIndexMap(hour) = new mutable.HashSet[Long]()
            while (hourCountIndexMap(hour).size < perHourCount) {
              //可能会有索引为0的项，取值时从0开始
              val index = random.nextInt(count.toInt)
              hourCountIndexMap(hour).add(index)
            }
          case Some(indexSet) =>
            while (indexSet.size < perHourCount) {
              //可能会有索引为0的项，取值时从0开始
              val index = random.nextInt(count.toInt)
              indexSet.add(index)
            }
        }
      }
    }

    //定义每天每小时的 index
    val perDateHourIndex = new mutable.HashMap[String, mutable.HashMap[String, mutable.Set[Long]]]()
    //计算每小时应该抽取的索引
    for ((date, hourCountMap) <- dateHourCountMap) {
      val countPerSession = hourCountMap.values.sum
      perDateHourIndex.get(date) match {
        case None => perDateHourIndex(date) = new mutable.HashMap[String, mutable.Set[Long]]
          getEtractRandomIndex(countPerSession, hourCountMap, perDateHourIndex(date))
        case Some(hourIndexMap) => getEtractRandomIndex(countPerSession, hourCountMap, hourIndexMap)
      }
    }

    //将每小时需要抽取的索引广播
    val perDateHourIndexBrodcast = sparkSession.sparkContext.broadcast(perDateHourIndex)


    //将每天每小时的访问聚合 2018-04-17_16 sessionInfo
    val dateHourAndCount = dateHourInfo.groupByKey()

    //TODO: flatMap map
    val extractSessionInfo = dateHourAndCount.flatMap {
      case (dateHour, sessions) => {
        val extractSession = new mutable.ArrayBuffer[SessionRandomExtract]()
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)
        //TODO: value(date)(hour)
        val perHourIndex = perDateHourIndexBrodcast.value(date)(hour)
        var index = 0
        for (sessionInfo <- sessions) {
          if (perHourIndex.contains(index)) {
            val sessionId = StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_SESSION_ID)
            val startTime = StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_START_TIME)
            val searchKeywords = StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            val clickCategoryIds = StringUtils.getFieldFromConcatString(sessionInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
            extractSession.append(new SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategoryIds))
          }
          index += 1
        }
        extractSession
      }
    }
    import sparkSession.implicits._
    saveRDDToMysql(extractSessionInfo.toDF, sparkSession, "session_extract")

  }

  /**
    * 将RDD 保存到数据库 对应表中 以新增方式
    *
    * @param extractSessionInfo
    * @param sparkSession
    * @param tableName
    */
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
    * 将累加器中的值保存在数据库
    *
    * @param sparkSession
    * @param taskUUID
    * @param value
    */
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

    import sparkSession.implicits._
    val aggRdd = sparkSession.sparkContext.makeRDD(Array(stat)).toDF()
    saveRDDToMysql(aggRdd, sparkSession, "session_aggr_stat")
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
