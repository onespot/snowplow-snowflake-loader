package com.snowplowanalytics.snowflake.loader.ast

sealed trait Insert

object Insert {
  case class InsertQuery(schema: String, table: String, columns: List[String], from: Select) extends Insert
}

