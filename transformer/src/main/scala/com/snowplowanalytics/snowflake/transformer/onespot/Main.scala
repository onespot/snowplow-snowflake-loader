package com.snowplowanalytics.snowflake.transformer.onespot

import com.snowplowanalytics.snowflake.core.ProcessManifest
import com.snowplowanalytics.snowflake.transformer.{StringSetAccumulator, Transformer}
import org.apache.spark.{SparkConf, SparkContext}

class Main {
  def main(args: Array[String]): Unit = {

    val appConfig = OnespotTransformerJobConfig.loadConfigFrom(args).get

    // Always use EMR Role role for manifest-access
    val s3 = ProcessManifest.getS3(appConfig.awsRegion)
    val dynamoDb = ProcessManifest.getDynamoDb(appConfig.awsRegion)
    val manifest = ProcessManifest.AwsProcessingManifest(s3, dynamoDb)

    // Eager SparkContext initializing to avoid YARN timeout
    val config = new SparkConf()
      .setAppName("snowflake-transformer")
      .setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(config)

    println(s"Onespot Snowflake Transformer: processing ${appConfig.manifestKey}. ${System.currentTimeMillis()}")
    manifest.add(appConfig.manifest, appConfig.manifestKey)
    val shredTypes = process(sc, appConfig.manifestKey, appConfig.inFolder, appConfig.outFolder)
    manifest.markProcessed(appConfig.manifest, appConfig.manifestKey, shredTypes, appConfig.outFolder)
    println(s"Onespot Snowflake Transformer: processed ${appConfig.manifestKey}. ${System.currentTimeMillis()}")
  }

  def process(sc: SparkContext, runId: String, input: String, output: String) = {
    val keysAggregator = new StringSetAccumulator
    sc.register(keysAggregator)

    val events = sc.textFile(input)

    val snowflake = events.map { event =>
      Transformer.transform(event) match {
        case (keys, transformed) =>
          keysAggregator.add(keys)
          transformed
      }
    }
    snowflake.saveAsTextFile(output)
    val keysFinal = keysAggregator.value.toList
    println(s"Shred types for  ${runId}: " + keysFinal.mkString(", "))
    keysAggregator.reset()
    keysFinal
  }
}
