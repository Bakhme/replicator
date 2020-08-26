package ru.philosophyit.bigdata.entity

import java.sql.Timestamp

case class ImportResult(
                         sourceTableName: String,
                         targetTableName: String,
                         readRowsCount: BigInt,
                         status: String,
                         statusMessage: String,
                         lastImportDatetime: Timestamp,
                         timeKey: String
                       )