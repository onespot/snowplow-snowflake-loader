/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.transformer

import org.specs2.Specification

class TransformerConfigSpec extends Specification { def is = s2"""
  Parse valid configuration $e1
  Fail to parse configuration with invalid s3 path $e2
  """

  def e1 = {
    val args = List(
      "--input", "s3n://snowflake/archive",
      "--output", "s3://snowflake-loader/output/",
      "--aws-access-key-id", "AAAA",
      "--aws-secret-access-key", "abcd",
      "--aws-region", "us-east-1",
      "--manifest-table", "snowflake-manifest").toArray

    val expected = TransformerConfig(
      "s3://snowflake/archive/",
      "s3://snowflake-loader/output/",
      "AAAA",
      "abcd",
      "us-east-1",
      "snowflake-manifest")

    TransformerConfig.parse(args) must beSome(Right(expected))
  }

  def e2 = {
    val args = List(
      "--input", "http://snowflake/archive",
      "--output", "https://snowflake-loader/output/",
      "--aws-access-key-id", "AAAA",
      "--aws-secret-access-key", "abcd",
      "--aws-region", "us-east-1",
      "--manifest-table", "snowflake-manifest").toArray

    val expected = "Bucket name [http://snowflake/archive] must start with s3:// prefix, " +
      "Bucket name [https://snowflake-loader/output/] must start with s3:// prefix"
    TransformerConfig.parse(args) must beSome(Left(expected))
  }
}
