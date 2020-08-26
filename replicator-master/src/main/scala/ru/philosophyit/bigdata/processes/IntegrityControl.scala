package ru.philosophyit.bigdata.processes

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.philosophyit.bigdata.connection.DBConnection

class IntegrityControl(implicit spark: SparkSession, connect: DBConnection) {

  /** Получение списка первичных ключей заданной таблицы из системных таблиц */
  def getPKColumns(tableName: String): Array[String] = {
    val query =
      s"""(SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner
         |FROM all_constraints cons, all_cons_columns cols
         |WHERE cols.table_name = '$tableName' AND cons.constraint_type = 'P'
         |AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
         |ORDER BY cols.table_name, cols.position) info""".stripMargin.replaceAll("\n", " ")
    spark.read.format("jdbc").options(connect.connectionConfig(query)).load()
      .select("COLUMN_NAME").collect().map(_.getString(0))
  }

  /** Проверка уникальности первичных ключов и отсутствие в них null значений */
  def checkPrimaryKeys(df: DataFrame, pkColumns: Array[String]): Boolean = {
    val cnt = df.count()
    pkColumns.forall(s => df.select(s).distinct().count() == cnt && df.select(s).where(col(s).isNull).count() == 0)
  }

  /** Получение количества строк таблицы */
  def getRowsCount(dbName: String, tableName: String): Long = {
    val query = s"(SELECT count(*) FROM $dbName.$tableName) info"
    spark.read.format("jdbc").options(connect.connectionConfig(query)).load()
      .collect()(0).getDecimal(0).longValueExact()
  }

  /** Проверка количества считанных и записанных строк */
  def checkRowsCount(dfcnt: Long, cnt: Long): Boolean = {
    cnt == dfcnt
  }

}
