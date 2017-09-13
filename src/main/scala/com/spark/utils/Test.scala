package com.spark.utils

import java.util.Date

import org.apache.commons.lang3.time.DateFormatUtils

object Test {
  def main(args: Array[String]): Unit = {
//    getFromMysql()
    testRowKey
  }

  def getFromMysql(): Unit ={
    val conn = MysqlManager.getMysqlManager.getConnection
    //statemenet
    val stm = conn.createStatement()
    val sql = "select * from tbl_user_visit_rs limit 10"
    val rs = stm.executeQuery(sql)
    //prepareStatement
    //    val sql = "select * from tbl_user_visit_rs limit 20"
    //    val pst = conn.prepareStatement(sql)
    //    val rs:ResultSet =pst.executeQuery()
    while (rs.next) {
      println(rs.getString(3))
    }
  }

  def testRowKey(): Unit ={
    val dt:Date = new Date
    val timeStamp = dt.getTime
    val timeFormatted = DateFormatUtils.format(dt, "yyyy-MM-dd HH:mm:ss")
    println(timeFormatted+" "+timeStamp+" "+Long.MaxValue)

    println(Long.MaxValue-timeStamp)
    println(Long.MaxValue-1514536080000L)
  }

}
