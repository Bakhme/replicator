package ru.philosophyit.bigdata.processes

import org.apache.spark.sql.SparkSession
import ru.philosophyit.bigdata.connection.DBConnection
import ru.philosophyit.bigdata.system.Parameters

import scala.io.Source

object TablesInfo {

  /** Считывает информацию о таблицах и колонках, которые будут реплицироваться
   *
   * @return словарь вида: название таблицы -> колонки через запятую
   */
  def apply()(implicit conf: Parameters, connect: DBConnection, spark: SparkSession): Map[String, String] =
    parseTable(s"${conf.TARGET_DATABASE}.${conf.TABLES_INFO_TAB}")

  /** Получает необходимую информацию из специальной таблицы в бд */
  def parseTable(fullTableName: String)(implicit connect: DBConnection, spark: SparkSession): Map[String, String] =
    spark.read.table(fullTableName).rdd
      .map(row => (row.getAs[String]("Source_table_name"), row.getAs[String]("Need_columns")))
      .reduceByKey((col1, col2) => s"$col1, $col2").collect().toMap

  /** Получает необходимую информацию из csv файла */
  def parseFile(filePath: String): Map[String, String] = {
    def using[A, B <: {def close(): Unit}](closeable: B)(f: B => A): A =
      try { f(closeable) } finally { closeable.close() }
    using(Source.fromFile(filePath)) {
      source =>
        source.getLines.map {
          _.split(";", -1) match {
            case Array(table, columns) => table -> columns
            case Array(table) => table -> "*"
            case _ => throw new Throwable("Broken string")
          }
        }.toMap
    }
  }

}
