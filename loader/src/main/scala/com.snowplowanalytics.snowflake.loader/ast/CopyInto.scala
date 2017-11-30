package com.snowplowanalytics.snowflake.loader.ast

import CopyInto._

case class CopyInto(schema: String, table: String, columns: List[String], from: From, credentials: Option[Common.AwsCreds], fileFormat: FileFormat)

object CopyInto {
  case class From(schema: String, stageName: String, path: String)
  case class FileFormat(schema: String, formatName: String)
}
