/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.transformer

import org.apache.spark.{SparkConf, SparkContext}
import com.snowplowanalytics.snowflake.core.ProcessManifest


object Main {
  def main(args: Array[String]): Unit = {
    TransformerConfig.parse(args) match {
      case Some(Right(appConfig)) =>

        val s3 = ProcessManifest.getS3(appConfig.awsAccessKey, appConfig.awsSecretKey, appConfig.awsRegion)
        val dynamoDb = ProcessManifest.getDynamoDb(appConfig.awsAccessKey, appConfig.awsSecretKey, appConfig.awsRegion)
        val manifest = ProcessManifest.AwsProcessingManifest(s3, dynamoDb)

        // Eager SparkContext initializing to avoid YARN timeout
        val config = new SparkConf()
          .setAppName("snowflake-transformer")
          .setIfMissing("spark.master", "local[*]")
        val sc = new SparkContext(config)

        // Get run folders that are not in RunManifest in any form
        val runFolders = manifest.getUnprocessed(appConfig.manifestTable, appConfig.enrichedInput)

        runFolders match {
          case Right(folders) =>
            val configs = folders.map(TransformerJobConfig(appConfig.enrichedInput, appConfig.enrichedOutput, _))
            TransformerJob.run(sc, manifest, appConfig.manifestTable, configs)
          case Left(error) =>
            println("Cannot get list of unprocessed folders")
            println(error)
            sys.exit(1)
        }


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
