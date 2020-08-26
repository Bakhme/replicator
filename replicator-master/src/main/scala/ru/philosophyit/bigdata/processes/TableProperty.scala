package ru.philosophyit.bigdata.processes

import org.apache.spark.sql.SparkSession
import ru.philosophyit.bigdata.connection.DBConnection
import ru.philosophyit.bigdata.system.Parameters

object TableProperty {

  /**Преобразует стандартные типы данных БД к типам данных, необходимых для закгрузки**/
  def apply(sourceTable: String, columns: String)(implicit connect: DBConnection, conf: Parameters, spark: SparkSession): String = {

    val query =
      s"""
         |SELECT COLUMN_NAME, DATA_TYPE,
         |    nvl(DATA_LENGTH, 0) as DATA_LENGTH,
         |    nvl(DATA_PRECISION, 38) as DATA_PRECISION,
         |    nvl(DATA_SCALE, 10) as DATA_SCALE,
         |    nvl(COLUMN_ID, 0) as COLUMN_ID
         |  FROM ALL_TAB_COLUMNS
         |  WHERE OWNER = ${conf.SOURCE_DATABASE}
         |    AND TABLE_NAME = $sourceTable
         |  ORDER BY COLUMN_ID""".stripMargin

    val cols_for_cast: Array[(String, String, Int, Int, Int, Int)] = connect.readDataSample(query)
      .collect().map(row =>
      (row.getAs[String]("COLUMN_NAME"), row.getAs[String]("DATA_TYPE"),
        row.getAs[Int]("DATA_LENGTH"), row.getAs[Int]("DATA_PRECISION"),
        row.getAs[Int]("DATA_SCALE"), row.getAs[Int]("COLUMN_ID"))
    )

    val all_fields: Array[String] = cols_for_cast.map(field =>
      field._2 match {
        case "DATE" => s"""CAST(${field._1} as STRING)"""  //по стандарту переведется в TimeStamp
        case "VARCHAR2" => s"""${field._1}""" //по стандарту переведтся в String
        case "NUMBER" => field._5 match {
          case 0 => s"""CAST(${field._1} as STRING)""" //по стандарту переведтся в Long
          case _ => s"""CAST(${field._1} as STRING)""" //по стандарту переведтся в Decimal(38,10)
        }
        case "FLOAT" => s"""CAST(${field._1} as STRING)""" //по стандарту переведтся в Decimal(38,10)
        case _ => s"""CAST(${field._1} as STRING)""" //для остальных непонятных типов данных
      })

    all_fields.mkString(",")
  }
}
