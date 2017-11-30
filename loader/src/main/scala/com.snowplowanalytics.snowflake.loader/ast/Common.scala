package com.snowplowanalytics.snowflake.loader.ast

object Common {
  case class AwsCreds(awsAccessKeyId: String, awsSecretKey: String)
}
