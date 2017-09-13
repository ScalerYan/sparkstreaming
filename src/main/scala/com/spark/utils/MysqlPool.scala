package com.spark.utils

import java.sql.Connection

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.typesafe.config.{Config, ConfigFactory}

class MysqlPool extends Serializable {
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  private val conf: Config= ConfigFactory.load()

  try {
    cpds.setJdbcUrl(conf.getString("mysql.url"))
    cpds.setDriverClass("com.mysql.jdbc.Driver")
    cpds.setUser(conf.getString("mysql.properties.user"))
    cpds.setPassword(conf.getString("mysql.properties.password"))
    cpds.setMaxPoolSize(200)
    cpds.setMinPoolSize(20)
    cpds.setAcquireIncrement(5)
    cpds.setMaxStatements(180)
  } catch {
    case e: Exception => e.printStackTrace()
  }

  def getConnection: Connection = {
    try {
      return cpds.getConnection();
    } catch {
      case ex:Exception => ex.printStackTrace()
        null
    }
  }

}
