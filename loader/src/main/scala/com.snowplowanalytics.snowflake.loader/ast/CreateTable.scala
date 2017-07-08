/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader.ast

import com.snowplowanalytics.snowflake.loader.ast.CreateTable._

case class CreateTable(
  schema: String,
  name: String,
  columns: List[Column],
  primaryKey: Option[PrimaryKeyConstraint],
  temporary: Boolean = false)

object CreateTable {
  case class PrimaryKeyConstraint(name: String, column: String)
}
