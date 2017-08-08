/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader.ast

case class Column(
  name: String,
  dataType: SnowflakeDatatype,
  notNull: Boolean = false,
  unique: Boolean = false)
