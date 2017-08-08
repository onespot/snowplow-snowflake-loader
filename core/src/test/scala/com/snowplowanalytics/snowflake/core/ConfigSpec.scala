package com.snowplowanalytics.snowflake.core

import org.specs2.Specification

class ConfigSpec extends Specification { def is = s2"""
  Parse valid S3 without trailing slash $e1
  Parse valid S3 with trailing slash and s3n scheme $e2
  Fail to parse invalid scheme $e3
  """

  def e1 = {
    val result = Config.S3Folder.parse("s3://cross-batch-test/archive/some-folder")
    result must beRight("s3://cross-batch-test/archive/some-folder/")
  }

  def e2 = {
    val result = Config.S3Folder.parse("s3n://cross-batch-test/archive/some-folder/")
    result must beRight("s3://cross-batch-test/archive/some-folder/")
  }

  def e3 = {
    val result = Config.S3Folder.parse("http://cross-batch-test/archive/some-folder/")
    result must beLeft("Bucket name [http://cross-batch-test/archive/some-folder/] must start with s3:// prefix")
  }
}
