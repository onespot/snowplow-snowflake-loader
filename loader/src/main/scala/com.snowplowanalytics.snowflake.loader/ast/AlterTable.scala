package com.snowplowanalytics.snowflake.loader.ast

sealed trait AlterTable {
  def schema: String
  def table: String
}

object AlterTable {
  case class DropColumn(schema: String, table: String, column: String) extends AlterTable
  case class AddColumn(schema: String, table: String, column: String, datatype: SnowflakeDatatype) extends AlterTable
}
