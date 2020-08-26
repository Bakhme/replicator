package ru.philosophyit.bigdata.system

import org.apache.log4j.Logger

class Parameters(args: Array[String])(implicit log: Logger) {

  val paramMap: Map[String, String] = args.flatMap(argLine => argLine.split("=", -1) match {
    case Array(key, value) if value.nonEmpty => Some(key -> value)
    case _ =>
      log.warn(s"Cannot parse parameter string or empty value: $argLine")
      None
  }).toMap

  log.info("=====" * 4)
  paramMap.foreach {
    case (k, v) => log.info(s"$k = $v")
  }
  log.info("=====" * 4)

  /** Connection parameters */
  val JDBC_URL: String = paramMap.getOrElse("JDBC_URL", "")
  val JDBC_DRIVER: String = paramMap.getOrElse("JDBC_DRIVER", "")
  val SOURCE_DATABASE: String = paramMap.getOrElse("DATABASE", "")
  val USER: String = paramMap.getOrElse("USER", "")
  val PASSWORD: String = paramMap.getOrElse("PASSWORD", "")

  /** Path to tables information */
  val TABLES_INFO_TAB: String = paramMap.getOrElse("TABLES_INFO_PATH", "")

  val TARGET_DATABASE: String = paramMap.getOrElse("HIVE_DATABASE", "")
  val TARGET_TABLES_PREFIX: String = paramMap.getOrElse("TARGET_TABLES_PREFIX", "")
  val REPLICATION_DATE: String = paramMap.getOrElse("REPLICATION_DATE", "")
  val LOGS_TABLE: String = paramMap.getOrElse("LOGS_TABLE", "")

}
