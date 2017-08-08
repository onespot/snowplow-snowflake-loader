/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader.ast

sealed trait CreateFileFormat

object CreateFileFormat {
  // TODO: add other format and options
  case class CreateCsvFormat(name: String, recordDelimiter: Option[String], fieldDelimiter: Option[String]) extends CreateFileFormat
}
