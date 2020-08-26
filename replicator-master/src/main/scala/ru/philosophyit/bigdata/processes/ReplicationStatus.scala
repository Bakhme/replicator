package ru.philosophyit.bigdata.processes

import java.sql.Timestamp
import java.time.LocalDateTime

import ru.philosophyit.bigdata.entity.ImportResult
import ru.philosophyit.bigdata.system.Parameters

case class ReplicationStatus(sourceTableName: String, targetTableName: String, readRowsCount: Long)
                            (implicit conf: Parameters) {

  private val lastImportDatetime: Timestamp = Timestamp.valueOf(LocalDateTime.now())
  private val timeKey: String = conf.REPLICATION_DATE

  def error(message: String): ImportResult = ImportResult(
    sourceTableName,
    targetTableName,
    readRowsCount,
    "ERROR",
    message,
    lastImportDatetime,
    timeKey
  )

  def success(message: String): ImportResult = ImportResult(
    sourceTableName,
    targetTableName,
    readRowsCount,
    "SUCCESS",
    message,
    lastImportDatetime,
    timeKey
  )

}
