package com.soap.flink.source

import java.sql.Types

import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.{JDBCInputFormat, JDBCOutputFormat}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
  * @author yangfuzhao on 2018/12/29. 
  */
object JDBCSource {


  val driverClass = "com.mysql.jdbc.Driver"
  val dbUrl = "jdbc:mysql://localhost:3306/or_test"
  val userNmae = "root"
  val passWord = "123456"

  def main(args: Array[String]): Unit = {
    // 运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 插入一组数据
    // 准备数据
    val row1 = new Row(3)
    row1.setField(0, "q")
    row1.setField(1, "maxiu")
    row1.setField(2, "123456")

    val row2 = new Row(3)
    row2.setField(0, "s")
    row2.setField(1, "蠢妹")
    row2.setField(2, "不知道")

    val rows: Array[Row] = Array(row1, row2)

    // 插入数据
    insertRows(rows)

    // 查看所有数据
//    selectAllFields(env)

//    // 更新某行
    //    val row22 = new Row(3)
    //    row22.setField(0, "s")
    //    row22.setField(1, "仙女")
    //    row22.setField(2, "哈哈哈")
    // updateRow(row22)
//    env.execute("a")


  }

  /**
    * 插入数据
    */
  def insertRows(rows: Array[Row]): Unit = {
    // 准备输出格式
    val jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
      .setDrivername(driverClass)
      .setDBUrl(dbUrl)
      .setUsername(userNmae)
      .setPassword(passWord)
      .setQuery("insert into person (first_name,last_name,deleted) values(?,?,?)")
      // 需要对应到表中的字段
      .setSqlTypes(Array[Int](Types.VARCHAR, Types.VARCHAR, Types.VARCHAR))
      .finish()

    // 连接到目标数据库，并初始化preparedStatement
    jdbcOutputFormat.open(0, 1)

    // 添加记录到 preparedStatement,此时jdbcOutputFormat需要确保是开启的
    // 未指定列类型时，此操作可能会失败
    for (row <- rows) {
      jdbcOutputFormat.writeRecord(row)
    }

    // 执行preparedStatement，并关闭此实例的所有资源
    jdbcOutputFormat.close()
  }


  /**
    * 更新某行数据（官网没给出更新示例，不知道实际是不是这样更新的）
    *
    * @param row 更新后的数据
    */
  def updateRow(row: Row): Unit = {
    // 准备输出格式
    val jdbcOutputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
      .setDrivername(driverClass)
      .setDBUrl(dbUrl)
      .setUsername(userNmae)
      .setPassword(passWord)
      .setQuery("update person set first_name = ?, last_name = ? where id = ?")
      // 需要对应到行rowComb中的字段类型
      .setSqlTypes(Array[Int](Types.VARCHAR, Types.VARCHAR, Types.VARCHAR))
      .finish()

    // 连接到目标数据库，并初始化preparedStatement
    jdbcOutputFormat.open(0, 1)

    // 组装sql中对应的字段，rowComb中的字段个数及类型需要与sql中的问号一致
    val rowComb = new Row(3)
    rowComb.setField(0, row.getField(1).asInstanceOf[String])
    rowComb.setField(1, row.getField(2).asInstanceOf[String])
    rowComb.setField(2, row.getField(0).asInstanceOf[Int])

    // 添加记录到 preparedStatement,此时jdbcOutputFormat需要确保是开启的
    // 未指定列类型时，此操作可能会失败
    jdbcOutputFormat.writeRecord(rowComb)

    // 执行preparedStatement，并关闭此实例的所有资源
    jdbcOutputFormat.close()
  }

  /**
    * 查询所有字段
    *
    * @return
    */
  def selectAllFields(env: ExecutionEnvironment) = {
    val inputBuilder = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername(driverClass)
      .setDBUrl(dbUrl)
      .setUsername(userNmae)
      .setPassword(passWord)
      .setQuery("select * from person")
      // 这里第一个字段类型写int会报类型转换异常。
      .setRowTypeInfo(new RowTypeInfo(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO))

    val source = env.createInput(inputBuilder.finish)
    source.print()

    env.execute("jdbc")
  }
}
