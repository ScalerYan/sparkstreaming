package com.spark.utils


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object HbaseUtil extends Serializable {
  private val config: Config = ConfigFactory.load()
  val conf: Configuration = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", config.getString("hbase.zkHosts"))
  conf.set("hbase.zookeeper.property.clientPort", config.getString("hbase.zkPort"))
  private val connection: Connection = ConnectionFactory.createConnection(conf)

  def getHbaseConn: Connection = connection
}
