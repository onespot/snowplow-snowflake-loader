/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.transformer

import org.apache.spark.{SparkConf, SparkContext}

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB

import com.snowplowanalytics.snowflake.core.ProcessManifest

object TransformerJob {

  /** Process all directories, saving state into DynamoDB */
  def run(dynamoDB: AmazonDynamoDB, tableName: String, jobConfigs: List[TransformerJobConfig]): Unit = {
    val config = new SparkConf()
      .setAppName("snowflake-transformer")
      .setIfMissing("spark.master", "local[*]")

    val sc = new SparkContext(config)

    jobConfigs.foreach { jobConfig =>
      ProcessManifest.add(dynamoDB, tableName, jobConfig.runId)
      val shredTypes = process(sc, jobConfig)
      ProcessManifest.markProcessed(dynamoDB, tableName, jobConfig.runId, shredTypes, jobConfig.output)
    }
  }

  /**
   * Transform particular folder to Snowflake-compatible format and
   * return list of discovered shredded types
   *
   * @param sc existing spark context
   * @param jobConfig configuration with paths
   * @return list of discovered shredded types
   */
  def process(sc: SparkContext, jobConfig: TransformerJobConfig) = {
    val keysAggregator = new StringSetAccumulator
    sc.register(keysAggregator)

    val events = sc.textFile(jobConfig.input)

    val snowflake = events.map { event =>
      Transformer.transform(event) match {
        case (keys, transformed) =>
          keysAggregator.add(keys)
          transformed
      }
    }

    snowflake.saveAsTextFile(jobConfig.output)

    val keysFinal = keysAggregator.value.toList
    println(s"Shred types for  ${jobConfig.runId}: " + keysFinal.mkString(", "))
    keysAggregator.reset()
    keysFinal
  }
}
