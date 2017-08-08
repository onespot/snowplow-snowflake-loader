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

import ast.CreateTable._
import ast.SnowflakeDatatype._

package object ast {
  implicit object DatatypeShow extends Show[SnowflakeDatatype] {
    def show(ddl: SnowflakeDatatype): String = ddl match {
      case Varchar(size)            => s"VARCHAR($size)"
      case Timestamp                => "TIMESTAMP"
      case Char(size)               => s"CHAR($size)"
      case SmallInt                 => "SMALLINT"
      case DoublePrecision          => "DOUBLE PRECISION"
      case Integer                  => "INTEGER"
      case Number(precision, scale) => s"NUMBER($precision,$scale)"
      case Boolean                  => "BOOLEAN"
      case Variant                  => "VARIANT"
      case JsonObject               => "OBJECT"
      case JsonArray                => "ARRAY"
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

  implicit object CastedColumnShow extends Show[Select.CastedColumn] {
    def show(column: Select.CastedColumn): String =
      s"${column.originColumn}:${column.columnName}::${column.datatype.show}"
  }

  implicit class StatementSyntax[S](val ast: S) extends AnyVal {
    def getStatement(implicit S: Statement[S]): Statement.SqlStatement =
      S.getStatement(ast)
  }
}
