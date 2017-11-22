/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.transformer

import org.apache.spark.{SparkConf, SparkContext}
import com.snowplowanalytics.snowflake.core.{ ProcessManifest, Config }


object Main {
  def main(args: Array[String]): Unit = {
    Config.parseTransformerCli(args) match {
      case Some(Right(Config.CliTransformerConfiguration(appConfig))) =>

        val s3 = ProcessManifest.getS3(appConfig.accessKeyId, appConfig.secretAccessKey, appConfig.awsRegion)
        val dynamoDb = ProcessManifest.getDynamoDb(appConfig.accessKeyId, appConfig.secretAccessKey, appConfig.awsRegion)
        val manifest = ProcessManifest.AwsProcessingManifest(s3, dynamoDb)

        // Eager SparkContext initializing to avoid YARN timeout
        val config = new SparkConf()
          .setAppName("snowflake-transformer")
          .setIfMissing("spark.master", "local[*]")
        val sc = new SparkContext(config)

        // Get run folders that are not in RunManifest in any form
        val runFolders = manifest.getUnprocessed(appConfig.manifest, appConfig.input)

        runFolders match {
          case Right(folders) =>
            val configs = folders.map(TransformerJobConfig(appConfig.input, appConfig.stageUrl, _))
            TransformerJob.run(sc, manifest, appConfig.manifest, configs)
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
