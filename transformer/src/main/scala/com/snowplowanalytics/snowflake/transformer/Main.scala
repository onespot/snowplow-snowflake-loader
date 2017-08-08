/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.transformer

import com.snowplowanalytics.snowflake.core.ProcessManifest


object Main {
  def main(args: Array[String]): Unit = {
    TransformerConfig.parse(args) match {
      case Some(Right(appConfig)) =>
        val runFolders = ProcessManifest.getUnprocessed(
          appConfig.awsAccessKey,
          appConfig.awsSecretKey,
          appConfig.awsRegion,
          appConfig.manifestTable,
          appConfig.enrichedInput)

        val configs = runFolders.map(TransformerJobConfig(appConfig.enrichedInput, appConfig.enrichedOutput, _))
        val dynamoDb = ProcessManifest.getDynamoDb(appConfig.awsAccessKey, appConfig.awsSecretKey)
        TransformerJob.run(dynamoDb, appConfig.manifestTable, configs)

      case Some(Left(error)) =>
        // Failed transformation
        println(error)
        sys.exit(1)

      case None =>
        // Invalid arguments
        sys.exit(1)
    }
  }
}
