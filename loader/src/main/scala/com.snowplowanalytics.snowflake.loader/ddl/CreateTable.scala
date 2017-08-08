/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader.ddl

import CreateTable._

case class CreateTable(
  schema: Option[String],
  name: String,
  columns: List[Column],
  primaryKey: Option[PrimaryKeyConstraint],
  temporary: Boolean = false) {
  def getFullTableName = schema match {
    case Some(s) => s"$s.$name"
    case None => name
  }
}

object CreateTable {
  case class PrimaryKeyConstraint(name: String, column: String)
}
