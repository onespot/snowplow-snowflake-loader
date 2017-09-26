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
      "--aws-region", "us-west-1",
      "--manifest-table", "strawberry-manifest",

      "--stage-url", "s3://strawberry/output",
      "--stage-name", "some_stage",

      "--user", "anton",
      "--password", "secret",
      "--account", "snowplow",
      "--warehouse", "snowplow_wh",
      "--schema", "atomic",
      "--db", "test_db").toArray

    val expected = LoaderConfig.SetupConfig(
      "AAAA", "abcd", "us-west-1", "strawberry-manifest",
      "s3://strawberry/output/", "some_stage",
      "anton", "secret", "snowplow", "snowplow_wh", "test_db", "atomic")

    LoaderConfig.parse(args) must beSome(Right(expected))
  }

  def e2 = {
    val args = List(
      "load",

      "--aws-access-key-id", "AAAA",
      "--aws-secret-access-key", "abcd",
      "--aws-region", "us-east-1",
      "--manifest-table", "strawberry-manifest",

      "--stage-name", "some_stage",

      "--user", "anton",
      "--password", "secret",
      "--account", "snowplow",
      "--warehouse", "snowplow_wh",
      "--schema", "atomic",
      "--db", "test_db").toArray

    val expected = LoaderConfig.LoadConfig(
      "AAAA", "abcd", "us-east-1", "strawberry-manifest",
      "some_stage", "anton", "secret", "snowplow", "snowplow_wh", "test_db", "atomic")

    LoaderConfig.parse(args) must beSome(Right(expected))
  }
}
