/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.transformer

import com.snowplowanalytics.snowflake.core.Config._

case class TransformerJobConfig(enrichedArchive: S3Folder, snowflakeOutput: S3Folder, runId: String) {
  def input: String = {
    val (enrichedBucket, enrichedPath) = enrichedArchive.splitS3Folder
    s"s3a://$enrichedBucket/$enrichedPath$runIdFolder/part-*"
  }

  def output: String = {
    val (bucket, path) = snowflakeOutput.splitS3Folder
    s"s3a://$bucket/$path$runIdFolder"
  }

  def runIdFolder: String = runId.split("/").last
}

