/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader

import cats.Show
import cats.implicits._

import ddl.CreateTable._
import ddl.SnowflakeDatatype._

package object ddl {
  implicit object DatatypeShow extends Show[SnowflakeDatatype] {
    def show(ddl: SnowflakeDatatype): String = ddl match {
      case Varchar(size)            => s"VARCHAR($size)"
      case Timesamp                 => "TIMESTAMP"
      case Char(size)               => s"CHAR($size)"
      case SmallInt                 => "SMALLINT"
      case DoublePrecision          => "DOUBLE PRECISION"
      case Integer                  => "INTEGER"
      case Number(precision, scale) => s"NUMBER($precision, $scale)"
      case Boolean                  => "BOOLEAN"
      case Variant                  => "VARIANT"
      case JsonObject               => "OBJECT"
    }
  }

  implicit object PrimaryKeyShow extends Show[PrimaryKeyConstraint] {
    def show(ddl: PrimaryKeyConstraint): String =
      s"CONSTRAINT ${ddl.name} PRIMARY KEY(${ddl.column})"
  }

  implicit object ColumnShow extends Show[Column] {
    def show(ddl: Column): String = {
      val datatype = ddl.dataType.show
      val constraints = ((if (ddl.notNull) "NOT NULL" else "") :: (if (ddl.unique) "UNIQUE" else "") :: Nil).filterNot(_.isEmpty)
      val renderedConstraints = if (constraints.isEmpty) "" else " " + constraints.mkString(" ")
      s"${ddl.name} $datatype" + renderedConstraints
    }
  }

  implicit object CreateTableShow extends Show[CreateTable] {
    def show(ddl: CreateTable): String = {
      val schema = ddl.schema.map(_ + ".").getOrElse("")
      val longest = ddl.columns.map(_.name.length).maximumOption.getOrElse(1)
      val constraint = ddl.primaryKey.map { p => ",\n\n  " + p.show }.getOrElse("")
      val cols = ddl.columns.map(_.show).map(_.split(" ").toList).map {
        case columnName :: tail => columnName + " " * (longest + 1 - columnName.length) + tail.mkString(" ")
        case other => other.mkString(" ")
      }
      val temporary = if (ddl.temporary) " TEMPORARY " else " "
      s"CREATE${temporary}TABLE IF NOT EXISTS $schema${ddl.name} (\n" +
        cols.map("  " + _).mkString(",\n") + constraint + "\n);"
    }
  }

  implicit object CreateSchemaShow extends Show[CreateSchema] {
    def show(ddl: CreateSchema): String =
      s"CREATE SCHEMA IF NOT EXISTS ${ddl.name}"
  }

  implicit object CreateFileFormatShow extends Show[CreateFileFormat] {
    def show(ddl: CreateFileFormat): String = ddl match {
      case CreateFileFormat.CreateCsvFormat(name, recordDelimiter, fieldDelimiter) =>
        val recordDelRendered = s"RECORD_DELIMITER = '${recordDelimiter.getOrElse("NONE")}'"
        val fieldDelRendered = s"FIELD_DELIMITER = '${fieldDelimiter.getOrElse("NONE")}'"
        s"CREATE FILE FORMAT IF NOT EXISTS $name TYPE = CSV $recordDelRendered $fieldDelRendered"
    }
  }

  implicit object CreateStageShow extends Show[CreateStage] {
    def show(ddl: CreateStage): String = {
      val schema = ddl.schema.map(_ + ".").getOrElse("")
      s"CREATE STAGE IF NOT EXISTS $schema${ddl.name} URL = '${ddl.url}' FILE_FORMAT = ${ddl.fileFormat}"
    }
  }

  implicit object CreateWarehouseShow extends Show[CreateWarehouse] {
    def show(ddl: CreateWarehouse): String = {
      val size = ddl.size.getOrElse(CreateWarehouse.DefaultSize).toString.toUpperCase
      s"CREATE WAREHOUSE IF NOT EXISTS ${ddl.name} WAREHOUSE_SIZE = $size"
    }
  }
}
