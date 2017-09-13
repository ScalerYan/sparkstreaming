package com.spark.sparkstreaming

import java.sql.{Connection, DriverManager}
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * /home/hadoop/spark-2.1.1/bin/spark-submit --class com.spark.sparkstreaming.PvCalculate --master spark://hadoop3:7077
  * --executor-cores 1 --executor-memory 1G
  * --total-executor-cores 3 /home/hadoop/data/test/sparkstreaming-pv-1.0.0.jar hadoop1:9092,hadoop2:9092,hadoop3:9092 user-visit userVisit
  */

object PvCalculate {
  val serialVersionUID: Long = 1113799434508676095L
  val sparkConf = new SparkConf()
  val logger = Logger.getLogger(PvCalculate.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics> <groupid>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <groupid> is a consume group
           |
            """.stripMargin)
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.WARN)

    val Array(brokers, topics, groupId) = args

    val master = "spark://hadoop3:7077"
    sparkConf.setAppName("PvCalculate").setMaster(master)
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "5")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val spark = SparkSession.builder().appName("PV").config("spark.master", master).getOrCreate()

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.131.102:3306/monitor_test"
    val usr = "root"
    val pwd = "123456"

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
    )

    val km = new KafkaManager(kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    //创建schema
    val schemaString = "uid page os"
    val fields = {
      schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    }
    val schema = StructType(fields)

    messages.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        // 先处理消息
        val rowRDD = rdd.map(_._2).map(_.split("\t")).map(attributes => Row(attributes(1).trim, attributes(3).trim,
          attributes(4).trim))
        // Apply the schema to the RDD
        val peopleDF = spark.createDataFrame(rowRDD, schema)
        peopleDF.createOrReplaceTempView("user_visit")

        val results = spark.sql("SELECT page,count(distinct uid),count(1) FROM user_visit group by page")
        implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
        val timeStamp = DateFormatUtils.format(new Date, "yyyy-MM-dd HH:mm:00")
        //对不同位置的广告进行分组，求pv和uv
        results.foreachPartition(
          dataSet => {
            Class.forName(driver)
            val conn: Connection = DriverManager.getConnection(url, usr, pwd)
            val preparedStat = conn.prepareStatement("insert into tbl_user_visit_rs(time_stamp,page,uv,pv) values (?,?,?,?)")
            for (data <- dataSet) {
              preparedStat.setString(1, timeStamp)
              preparedStat.setString(2, data(0).toString)
              preparedStat.setInt(3, Integer.valueOf(data(1).toString))
              preparedStat.setInt(4, Integer.valueOf(data(2).toString))
              preparedStat.executeUpdate()
            }
            conn.close()
          }
        )
        // 再更新offsets
        km.updateZKOffsets(rdd)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def processRdd(rdd: RDD[(String, String)]): Unit = {
    val lines = rdd.map(_._2)
    val words = lines.map(x => x.split("\t").toList(2))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.foreach(println)
  }
}
