package ru.philosophyit.bigdata.processes

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import ru.philosophyit.bigdata.connection.DBConnection
import ru.philosophyit.bigdata.entity.ImportResult
import ru.philosophyit.bigdata.system.Parameters

object ImportTables {

  /** Запускает репликацию таблиц
   *
   * @param loadList словарь: ключи - имена таблиц, значения - колонки для репликации
   * @return датасет из записей о состояние каждой из репликаций(прошла успешно/неуспешно)
   */
  def apply(loadList: Map[String, String])
           (implicit spark: SparkSession, conf: Parameters,
            connect: DBConnection, log: Logger): Dataset[ImportResult] = {
    import spark.implicits._

    log.info("Start full replication")

    val resultStatus = for ((table, columns) <- loadList)
      yield ImportTable(table, columns).result

    log.info("Full replication done")
    spark.sparkContext.parallelize(resultStatus.toSeq).toDS()
  }

}
