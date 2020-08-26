package ru.philosophyit.bigdata

import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import ru.philosophyit.bigdata.connection.DBConnection
import ru.philosophyit.bigdata.processes.{ImportTables, TablesInfo}
import ru.philosophyit.bigdata.system.Parameters

object Main {

  def main(args: Array[String]): Unit = {
    implicit val log: Logger = Logger.getLogger(getClass)
    implicit val conf: Parameters = new Parameters(args)
    implicit val spark: SparkSession = SparkSession.builder
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()
    implicit val connect: DBConnection = new DBConnection

    val loadList = TablesInfo()
    ImportTables(loadList).write.mode(SaveMode.Append)
      .insertInto(s"${conf.SOURCE_DATABASE}.${conf.LOGS_TABLE}")

    spark.close()
  }

}