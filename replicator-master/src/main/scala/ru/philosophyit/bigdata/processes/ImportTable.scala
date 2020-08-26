package ru.philosophyit.bigdata.processes

import org.apache.spark.sql.{SaveMode, SparkSession}
import ru.philosophyit.bigdata.connection.DBConnection
import ru.philosophyit.bigdata.entity.ImportResult
import ru.philosophyit.bigdata.system.Parameters

object ImportTable {

  private var isFailure: Boolean = false
  private var message: String = "Insert table success!"
  private var readRowsCount = 0L

  /** Реплицирует одну указанную таблицу
   *
   * @param sourceTable название таблицы
   * @param columns извлекаемые колонки
   * @return результат репликации(прошла успешно/неуспешно)
   */
  def apply(sourceTable: String, columns: String)(implicit connect: DBConnection, conf: Parameters, spark: SparkSession): ImportTable = {
    val targetTable = s"${conf.TARGET_TABLES_PREFIX}_$sourceTable"
    try {
      val df = connect.readData(conf.SOURCE_DATABASE, sourceTable, TableProperty(sourceTable, columns))
        .persist()

      readRowsCount = df.count()
      df.write.mode(SaveMode.Overwrite).saveAsTable(s"${conf.TARGET_DATABASE}.$targetTable")

      val intCon = new IntegrityControl()
      intCon.checkRowsCount(readRowsCount, intCon.getRowsCount(conf.SOURCE_DATABASE, sourceTable)) match {
        case false => isFailure = true
          message = "Wrong rows count"
      }

      df.unpersist()
    } catch {
      case e: Exception =>
        isFailure = true
        message = e.getMessage
    }

    val resultStatus = ReplicationStatus(sourceTable, targetTable, readRowsCount)
    if (isFailure) ImportTable(resultStatus.error(message))
    else ImportTable(resultStatus.success(message))
  }
}

case class ImportTable(result: ImportResult)