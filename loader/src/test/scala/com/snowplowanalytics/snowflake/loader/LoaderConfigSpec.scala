/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader

import org.specs2.Specification

class LoaderConfigSpec extends Specification { def is = s2"""
  Parse valid setup configuration $e1
  Parse valid load configuration $e2
  """

  def e1 = {
    val args = List(
      "setup",

      "--aws-access-key-id", "AAAA",
      "--aws-secret-access-key", "abcd",
      "--manifest-table", "snowflake-manifest",

      "--stage-url", "s3://snowflake/output",
      "--stage-name", "some_stage",

      "--snowflake-region", "us-west-1",
      "--user", "anton",
      "--password", "secret",
      "--account", "snowplow",
      "--warehouse", "snowplow_wh",
      "--schema", "atomic",
      "--db", "test_db").toArray

    val expected = LoaderConfig.SetupConfig(
      awsAccessKey = "AAAA",
      awsSecretKey = "abcd",
      manifestTable = "snowflake-manifest",
      stageUrl = "s3://snowflake/output/",
      snowflakeStage = "some_stage",
      snowflakeRegion = "us-west-1",
      snowflakeUser = "anton",
      snowflakePassword = "secret",
      snowflakeAccount = "snowplow",
      snowflakeWarehouse = "snowplow_wh",
      snowflakeDb = "test_db",
      snowflakeSchema = "atomic")

    LoaderConfig.parse(args) must beSome(Right(expected))
  }

  def e2 = {
    val args = List(
      "load",

      "--aws-access-key-id", "AAAA",
      "--aws-secret-access-key", "abcd",
      "--aws-region", "us-east-1",
      "--manifest-table", "snowflake-manifest",

      "--stage-name", "some_stage",

      "--snowflake-region", "us-west-1",
      "--user", "anton",
      "--password", "secret",
      "--account", "snowplow",
      "--warehouse", "snowplow_wh",
      "--schema", "atomic",
      "--db", "test_db").toArray

    val expected = LoaderConfig.LoadConfig(
      "AAAA", "abcd", "us-east-1", "snowflake-manifest",
      "us-west-1", "some_stage", "anton", "secret", "snowplow", "snowplow_wh", "test_db", "atomic")

    LoaderConfig.parse(args) must beSome(Right(expected))
  }
}
