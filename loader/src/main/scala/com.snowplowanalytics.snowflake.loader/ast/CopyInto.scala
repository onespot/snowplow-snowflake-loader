package com.snowplowanalytics.snowflake.loader.ast

import CopyInto._

case class CopyInto(schema: String, table: String, columns: List[String], from: From, credentials: AwsCreds, fileFormat: FileFormat)

object CopyInto {
  case class From(schema: String, stageName: String, path: String)
  case class AwsCreds(awsAccessKeyId: String, awsSecretKey: String)
  case class FileFormat(schema: String, formatName: String)
}
