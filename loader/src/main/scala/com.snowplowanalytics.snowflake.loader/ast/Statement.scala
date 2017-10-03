/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader.ast

import cats.implicits._

trait Statement[-S] {
  def getStatement(ast: S): Statement.SqlStatement
}

object Statement {

  final case class SqlStatement private(value: String) extends AnyVal

  implicit object CreateTableStatement extends Statement[CreateTable] {
    def getStatement(ddl: CreateTable): SqlStatement = {
      val longest = ddl.columns.map(_.name.length).maximumOption.getOrElse(1)
      val constraint = ddl.primaryKey.map { p => ",\n\n  " + p.show }.getOrElse("")
      val cols = ddl.columns.map(_.show).map(_.split(" ").toList).map {
        case columnName :: tail => columnName + " " * (longest + 1 - columnName.length) + tail.mkString(" ")
        case other => other.mkString(" ")
      }
      val temporary = if (ddl.temporary) " TEMPORARY " else " "
      SqlStatement(s"CREATE${temporary}TABLE IF NOT EXISTS ${ddl.schema}.${ddl.name} (\n" +
        cols.map("  " + _).mkString(",\n") + constraint + "\n)"
      )
    }
  }

  implicit object CreateSchemaStatement extends Statement[CreateSchema] {
    def getStatement(ddl: CreateSchema): SqlStatement =
      SqlStatement(s"CREATE SCHEMA IF NOT EXISTS ${ddl.name}")
  }

  implicit object CreateFileFormatStatement extends Statement[CreateFileFormat] {
    def getStatement(ddl: CreateFileFormat): SqlStatement = ddl match {
      case CreateFileFormat.CreateCsvFormat(name, recordDelimiter, fieldDelimiter) =>
        val recordDelRendered = s"RECORD_DELIMITER = '${recordDelimiter.getOrElse("NONE")}'"
        val fieldDelRendered = s"FIELD_DELIMITER = '${fieldDelimiter.getOrElse("NONE")}'"
        SqlStatement(s"CREATE FILE FORMAT IF NOT EXISTS $name TYPE = CSV $recordDelRendered $fieldDelRendered")
      case CreateFileFormat.CreateJsonFormat(name) =>
        SqlStatement(s"CREATE FILE FORMAT IF NOT EXISTS $name TYPE = JSON")
    }
  }

  implicit object CreateStageStatement extends Statement[CreateStage] {
    def getStatement(ddl: CreateStage): SqlStatement = {
      SqlStatement(
        s"CREATE STAGE IF NOT EXISTS ${ddl.schema}.${ddl.name} URL = '${ddl.url}' FILE_FORMAT = ${ddl.fileFormat}"
      )
    }
  }

  implicit object CreateWarehouseStatement extends Statement[CreateWarehouse] {
    def getStatement(ddl: CreateWarehouse): SqlStatement = {
      val size = ddl.size.getOrElse(CreateWarehouse.DefaultSize).toString.toUpperCase
      SqlStatement(s"CREATE WAREHOUSE IF NOT EXISTS ${ddl.name} WAREHOUSE_SIZE = $size")
    }
  }

  implicit object SelectStatement extends Statement[Select] {
    def getStatement(ddl: Select): SqlStatement =
      SqlStatement(s"SELECT ${ddl.columns.map(_.show).mkString(", ")} FROM ${ddl.schema}.${ddl.table}")
  }

  implicit object InsertStatement extends Statement[Insert] {
    def getStatement(ddl: Insert): SqlStatement = ddl match {
      case Insert.InsertQuery(schema, table, columns, from) =>
        SqlStatement(s"INSERT INTO $schema.$table(${columns.mkString(",")}) ${from.getStatement.value}")
    }
  }

  implicit object AlterTableStatement extends Statement[AlterTable] {
    def getStatement(ast: AlterTable): SqlStatement = ast match {
      case AlterTable.AddColumn(schema, table, column, datatype) =>
        SqlStatement(s"ALTER TABLE $schema.$table ADD COLUMN $column ${datatype.show}")
      case AlterTable.DropColumn(schema, table, column) =>
        SqlStatement(s"ALTER TABLE $schema.$table DROP COLUMN $column")
    }
  }

  implicit object AlterWarehouseStatement extends Statement[AlterWarehouse] {
    def getStatement(ast: AlterWarehouse): SqlStatement = ast match {
      case AlterWarehouse.Resume(warehouse) =>
        SqlStatement(s"ALTER WAREHOUSE $warehouse RESUME")
    }
  }

  implicit object UseWarehouseStatement extends Statement[UseWarehouse] {
    def getStatement(ast: UseWarehouse): SqlStatement =
      SqlStatement(s"USE WAREHOUSE ${ast.warehouse}")
  }

  implicit object CopyInto extends Statement[CopyInto] {
    def getStatement(ast: CopyInto): SqlStatement =
      SqlStatement(s"COPY INTO ${ast.schema}.${ast.table}(${ast.columns.mkString(",")}) FROM @${ast.from.schema}.${ast.from.stageName}/${ast.from.path} CREDENTIALS = (AWS_KEY_ID = '${ast.credentials.awsAccessKeyId}' AWS_SECRET_KEY = '${ast.credentials.awsSecretKey}') FILE_FORMAT = (FORMAT_NAME = '${ast.fileFormat.schema}.${ast.fileFormat.formatName}')" )
  }

  implicit object ShowStageStatement extends Statement[Show.ShowStages] {
    def getStatement(ast: Show.ShowStages): SqlStatement = {
      val schemaPattern = ast.pattern.map(s => s" LIKE '$s'").getOrElse("")
      val scopePattern = ast.schema.map(s => s" IN $s").getOrElse("")
      SqlStatement(s"SHOW stages $schemaPattern$scopePattern")
    }
  }

  implicit object ShowSchemasStatement extends Statement[Show.ShowSchemas] {
    def getStatement(ast: Show.ShowSchemas): SqlStatement = {
      val schemaPattern = ast.pattern.map(s => s" LIKE '$s'").getOrElse("")
      SqlStatement(s"SHOW schemas $schemaPattern")
    }
  }

  implicit object ShowTablesStatement extends Statement[Show.ShowTables] {
    def getStatement(ast: Show.ShowTables): SqlStatement = {
      val schemaPattern = ast.pattern.map(s => s" LIKE '$s'").getOrElse("")
      val scopePattern = ast.schema.map(s => s" IN $s").getOrElse("")
      SqlStatement(s"SHOW tables $schemaPattern$scopePattern")
    }
  }

  implicit object ShowFileFormatsStatement extends Statement[Show.ShowFileFormats] {
    def getStatement(ast: Show.ShowFileFormats): SqlStatement = {
      val pattern = ast.pattern.map(s => s" LIKE '$s'").getOrElse("")
      val scopePattern = ast.schema.map(s => s" IN $s").getOrElse("")
      SqlStatement(s"SHOW file formats$pattern$scopePattern")
    }
  }

  implicit object ShowWarehousesStatement extends Statement[Show.ShowWarehouses] {
    def getStatement(ast: Show.ShowWarehouses): SqlStatement = {
      val pattern = ast.pattern.map(s => s" LIKE '$s'").getOrElse("")
      SqlStatement(s"SHOW warehouses$pattern")
    }
  }

  implicit object ShowColumnsStatement extends Statement[Show.ShowColumns] {
    def getStatement(ast: Show.ShowColumns): SqlStatement = {
      val pattern = ast.pattern.map(s => s" LIKE '$s'").getOrElse("")
      val scope = ast.schema match {
        case Some(schema) => ast.table match {
          case Some(table) => s" IN $schema.$table"
          case None => s" IN ${ast.table}"
        }
        case None => ast.table match {
          case Some(table) => s" IN $table"
          case None => ""
        }
      }

      SqlStatement(s"SHOW columns$pattern$scope")
    }
  }
}
