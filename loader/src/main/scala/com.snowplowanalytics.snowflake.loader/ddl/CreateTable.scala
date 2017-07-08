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
  primaryKey: Option[PrimaryKeyConstraint])

object CreateTable {
  case class PrimaryKeyConstraint(name: String, column: String)
}
