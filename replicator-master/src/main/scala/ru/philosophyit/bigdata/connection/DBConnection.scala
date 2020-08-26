package ru.philosophyit.bigdata.connection

import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.philosophyit.bigdata.entity.ReadingConfiguration
import ru.philosophyit.bigdata.system.Parameters

class DBConnection(implicit spark: SparkSession, log: Logger, conf: Parameters) {

  def connectionConfig(query: String): Map[String, String] = Map(
    "url" -> conf.JDBC_URL,
    "driver" -> conf.JDBC_DRIVER,
    "dbtable" -> query,
    "user" -> conf.USER,
    "password" -> conf.PASSWORD
  )

  def predicates(rc: ReadingConfiguration): Array[String] = {
    val step = (rc.upperBound - rc.lowerBound) / rc.numPartitions
    (for (num <- 0 until rc.numPartitions)
      yield s"${rc.partitionColumn} between ${rc.lowerBound + step * num} and ${rc.lowerBound + (step * (num + 1))}"
      ).toArray
  }

  def connectionProperties: Properties = {
    val cp = new Properties()
    cp.put("user", conf.USER)
    cp.put("password", conf.PASSWORD)
    cp.put("jdbcDriver", conf.JDBC_DRIVER)
    cp
  }

  def readData(dbName: String, tbName: String, columns: String, readType: String = "Sample"): DataFrame = {
    val readingConf = ReadingConfiguration("", 0, 0, 0)
    val query = s"(select $columns from $dbName.$tbName) info"
    log.info(s"Executing query: $query")
    readType match {
      case "Sample" => readDataSample(query)
      case "Config1" => readDataConfig1(query, readingConf)
      case "Config2" => readDataConfig2(query, readingConf)
      case "Config3" => readDataConfig3(query, readingConf)
      case _ => throw new Exception("No such read type")
    }
  }

  def readDataSample(query: String): DataFrame = spark.read
    .format("jdbc")
    .options(connectionConfig(query))
    .load()

  def readDataConfig1(query: String, readingConf: ReadingConfiguration): DataFrame = spark.read
    .format("jdbc")
    .options(connectionConfig(query))
    .option("partitionColumn", readingConf.partitionColumn)
    .option("lowerBound", readingConf.lowerBound)
    .option("upperBound", readingConf.upperBound)
    .option("numPartitions", readingConf.numPartitions)
    .option("oracle.jdbc.mapDateToTimestamp", "false")
    .option("sessionInitStatement", "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'")
    .load()

  def readDataConfig2(query: String, readingConf: ReadingConfiguration): DataFrame = spark.read.jdbc(
    url = conf.JDBC_URL,
    table = query,
    columnName = readingConf.partitionColumn,
    lowerBound = readingConf.lowerBound,
    upperBound = readingConf.upperBound,
    numPartitions = readingConf.numPartitions,
    connectionProperties = connectionProperties
  )

  def readDataConfig3(query: String, readingConf: ReadingConfiguration): DataFrame = spark.read.jdbc(
    url = conf.JDBC_URL,
    table = query,
    predicates = predicates(readingConf),
    connectionProperties = connectionProperties
  )

}
