/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.core

import org.specs2.Specification

import com.snowplowanalytics.snowflake.core.Config.S3Folder.{coerce => s3}
import com.snowplowanalytics.snowflake.core.Config.CliLoaderConfiguration

class ConfigSpec extends Specification { def is = s2"""
  Parse valid setup configuration $e1
  Parse valid load configuration $e2
  Parse valid base64-encoded configuration $e3

  Parse valid S3 without trailing slash $e4
  Parse valid S3 with trailing slash and s3n scheme $e5
  Fail to parse invalid scheme $e6
  Parse valid base64-encoded configuration without credentials $e7
  """

  val configUrl = getClass.getResource("/valid-config.json")
  val resolverUrl = getClass.getResource("/resolver.json")

  def e1 = {
    val args = List(
      "setup",

      "--resolver", s"${resolverUrl.getPath}",
      "--config", s"${configUrl.getPath}"
    ).toArray

    val expected = CliLoaderConfiguration(
      Config.SetupCommand,
      Config(
        accessKeyId = "ABCD",
        secretAccessKey = "abcd",
        awsRegion = "us-east-1",
        manifest = "snowflake-manifest",
        stage = "some_stage",
        stageUrl = s3("s3://snowflake/output/"),
        snowflakeRegion = "us-west-1",
        username = "anton",
        password = "Supersecret2",
        input = s3("s3://snowflake/input/"),
        account = "snowplow",
        warehouse = "snowplow_wh",
        database = "test_db",
        schema = "atomic"),
      false)

    Config.parseLoaderCli(args) must beSome(Right(expected))
  }

  def e2 = {
    val args = List(
      "load",

      "--dry-run",
      "--resolver", s"${resolverUrl.getPath}",
      "--config", s"${configUrl.getPath}").toArray

    val expected = CliLoaderConfiguration(
      Config.LoadCommand,
      Config(
        accessKeyId =  "ABCD",
        secretAccessKey = "abcd",
        awsRegion = "us-east-1",

        manifest = "snowflake-manifest",
        stage = "some_stage",
        stageUrl = s3("s3://snowflake/output/"),
        snowflakeRegion = "us-west-1",
        input = s3("s3://snowflake/input/"),
        schema = "atomic",
        username = "anton",
        password = "Supersecret2",
        account = "snowplow",
        warehouse = "snowplow_wh",
        database = "test_db"),
      true)

    Config.parseLoaderCli(args) must beSome(Right(expected))
  }

  def e3 = {
    val args = List(
      "load",

      "--dry-run",
      "--base64",
      "--resolver", "eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5pZ2x1L3Jlc29sdmVyLWNvbmZpZy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6eyJjYWNoZVNpemUiOjUsInJlcG9zaXRvcmllcyI6W3sibmFtZSI6IklnbHUgQ2VudHJhbCBiYXNlNjQiLCJwcmlvcml0eSI6MCwidmVuZG9yUHJlZml4ZXMiOlsiY29tLnNub3dwbG93YW5hbHl0aWNzIl0sImNvbm5lY3Rpb24iOnsiaHR0cCI6eyJ1cmkiOiJodHRwOi8vaWdsdWNlbnRyYWwuY29tIn19fV19fQ==",
      "--config", "eyAic2NoZW1hIjogImlnbHU6Y29tLnNub3dwbG93YW5hbHl0aWNzLnNub3dwbG93LnN0b3JhZ2Uvc25vd2ZsYWtlX2NvbmZpZy9qc29uc2NoZW1hLzEtMC0wIiwgImRhdGEiOiB7ICJuYW1lIjogIlNub3dmbGFrZSIsICJhY2Nlc3NLZXlJZCI6ICJBQkNEIiwgInNlY3JldEFjY2Vzc0tleSI6ICJhYmNkIiwgImF3c1JlZ2lvbiI6ICJ1cy1lYXN0LTEiLCAibWFuaWZlc3QiOiAic25vd2ZsYWtlLW1hbmlmZXN0IiwgInNub3dmbGFrZVJlZ2lvbiI6ICJ1cy13ZXN0LTEiLCAiZGF0YWJhc2UiOiAidGVzdF9kYiIsICJpbnB1dCI6ICJzMzovL3Nub3dmbGFrZS9pbnB1dC8iLCAic3RhZ2UiOiAic29tZV9zdGFnZSIsICJzdGFnZVVybCI6ICJzMzovL3Nub3dmbGFrZS9vdXRwdXQvIiwgIndhcmVob3VzZSI6ICJzbm93cGxvd193aCIsICJzY2hlbWEiOiAiYXRvbWljIiwgImFjY291bnQiOiAic25vd3Bsb3ciLCAidXNlcm5hbWUiOiAiYW50b24iLCAicGFzc3dvcmQiOiAiU3VwZXJzZWNyZXQyIiwgInB1cnBvc2UiOiAiRU5SSUNIRURfRVZFTlRTIiB9IH0="
    ).toArray

    val expected = CliLoaderConfiguration(
      Config.LoadCommand,
      Config(
        accessKeyId =  "ABCD",
        secretAccessKey = "abcd",
        awsRegion = "us-east-1",

        manifest = "snowflake-manifest",
        stage = "some_stage",
        stageUrl = s3("s3://snowflake/output/"),
        snowflakeRegion = "us-west-1",
        schema = "atomic",
        username = "anton",
        password = "Supersecret2",
        input = s3("s3://snowflake/input/"),
        account = "snowplow",
        warehouse = "snowplow_wh",
        database = "test_db"),
      true)

    Config.parseLoaderCli(args) must beSome(Right(expected))

  }

  def e4 = {
    val result = Config.S3Folder.parse("s3://cross-batch-test/archive/some-folder")
    result must beRight(s3("s3://cross-batch-test/archive/some-folder/"))
  }

  def e5 = {
    val result = Config.S3Folder.parse("s3n://cross-batch-test/archive/some-folder/")
    result must beRight(s3("s3://cross-batch-test/archive/some-folder/"))
  }

  def e6 = {
    val result = Config.S3Folder.parse("http://cross-batch-test/archive/some-folder/")
    result must beLeft("Bucket name [http://cross-batch-test/archive/some-folder/] must start with s3:// prefix")
  }

  def e7 = {
    val args = List(
      "setup",

      "--resolver", "eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5pZ2x1L3Jlc29sdmVyLWNvbmZpZy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6eyJjYWNoZVNpemUiOjUsInJlcG9zaXRvcmllcyI6W3sibmFtZSI6IklnbHUgQ2VudHJhbCBiYXNlNjQiLCJwcmlvcml0eSI6MCwidmVuZG9yUHJlZml4ZXMiOlsiY29tLnNub3dwbG93YW5hbHl0aWNzIl0sImNvbm5lY3Rpb24iOnsiaHR0cCI6eyJ1cmkiOiJodHRwOi8vaWdsdWNlbnRyYWwuY29tIn19fV19fQ==",
      "--config", "eyAic2NoZW1hIjogImlnbHU6Y29tLnNub3dwbG93YW5hbHl0aWNzLnNub3dwbG93LnN0b3JhZ2Uvc25vd2ZsYWtlX2NvbmZpZy9qc29uc2NoZW1hLzEtMC0wIiwgImRhdGEiOiB7ICJuYW1lIjogIlNub3dmbGFrZSIsICJhY2Nlc3NLZXlJZCI6IG51bGwsICJzZWNyZXRBY2Nlc3NLZXkiOiBudWxsLCAiYXdzUmVnaW9uIjogInVzLWVhc3QtMSIsICJtYW5pZmVzdCI6ICJzbm93Zmxha2UtbWFuaWZlc3QiLCAic25vd2ZsYWtlUmVnaW9uIjogInVzLXdlc3QtMSIsICJkYXRhYmFzZSI6ICJ0ZXN0X2RiIiwgImlucHV0IjogInMzOi8vc25vd2ZsYWtlL2lucHV0LyIsICJzdGFnZSI6ICJzb21lX3N0YWdlIiwgInN0YWdlVXJsIjogInMzOi8vc25vd2ZsYWtlL291dHB1dC8iLCAid2FyZWhvdXNlIjogInNub3dwbG93X3doIiwgInNjaGVtYSI6ICJhdG9taWMiLCAiYWNjb3VudCI6ICJzbm93cGxvdyIsICJ1c2VybmFtZSI6ICJhbnRvbiIsICJwYXNzd29yZCI6ICJTdXBlcnNlY3JldDIiLCAicHVycG9zZSI6ICJFTlJJQ0hFRF9FVkVOVFMiIH0gfQ==",
      "--base64"
    ).toArray

    val expected = CliLoaderConfiguration(
      Config.SetupCommand,
      Config(
        accessKeyId =  None,
        secretAccessKey = None,
        awsRegion = "us-east-1",

        manifest = "snowflake-manifest",
        stage = "some_stage",
        stageUrl = s3("s3://snowflake/output/"),
        snowflakeRegion = "us-west-1",
        schema = "atomic",
        username = "anton",
        password = "Supersecret2",
        input = s3("s3://snowflake/input/"),
        account = "snowplow",
        warehouse = "snowplow_wh",
        database = "test_db"),
      false)

    Config.parseLoaderCli(args) must beSome(Right(expected))
  }
}
