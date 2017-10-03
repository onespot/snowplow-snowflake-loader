package com.snowplowanalytics.snowflake.loader.ast

/**
  * ASTs for SHOW-queries
  */
object Show {
  case class ShowStages(pattern: Option[String], schema: Option[String])

  case class ShowSchemas(pattern: Option[String])

  case class ShowTables(pattern: Option[String], schema: Option[String])

  case class ShowFileFormats(pattern: Option[String], schema: Option[String])

  case class ShowWarehouses(pattern: Option[String])

  case class ShowColumns(pattern: Option[String], schema: Option[String], table: Option[String])
}
