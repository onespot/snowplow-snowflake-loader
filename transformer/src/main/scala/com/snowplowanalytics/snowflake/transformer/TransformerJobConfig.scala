/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.transformer

import com.snowplowanalytics.snowflake.core.Config.{ S3Folder, splitS3Folder }

case class TransformerJobConfig(enrichedArchive: S3Folder, snowflakeOutput: S3Folder, runId: String) {
  def input: String = {
    val (enrichedBucket, enrichedPath) = splitS3Folder(enrichedArchive)
    s"s3a://$enrichedBucket/$enrichedPath$runIdFolder/part-*"
  }

  def output: String = {
    val (bucket, path) = splitS3Folder(snowflakeOutput)
    s"s3a://$bucket/$path$runIdFolder"
  }

  def runIdFolder: String = runId.split("/").last
}

