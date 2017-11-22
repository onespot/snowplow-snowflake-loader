/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader

import scala.util.Random.shuffle

import org.specs2.Specification

import org.joda.time.DateTime

import com.snowplowanalytics.snowflake.core.RunId._
import com.snowplowanalytics.snowflake.core.Config.S3Folder.{ coerce => s3 }
import com.snowplowanalytics.snowflake.loader.SnowflakeState.FolderToLoad

class SnowflakeStateSpec extends Specification { def is = s2"""
  Correctly fold and sort list of processed folders into SnowflakeState $e1
  Correctly fold and sort list of loaded folders into SnowflakeState $e2
  Correctly fold and sort list of run ids into SnowflakeState $e3
  Correctly extract list of folders to load $e4
  """

  def e1 = {
    val initTime = 1502357136000L
    val input = List(
      ProcessedRunId("enriched/archived/run-01", new DateTime(initTime + 1000), new DateTime(1502368136000L + 3000), List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"), s3("s3://transformer-output/run-01/"), "transformer-1", false),
      ProcessedRunId("enriched/archived/run-02", new DateTime(initTime + 2000), new DateTime(1502368136000L + 5000), List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"), s3("s3://transformer-output/run-02/"), "transformer-1", false),
      ProcessedRunId("enriched/archived/run-03", new DateTime(initTime + 3000), new DateTime(1502368136000L + 6000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-03/"), "transformer-2", false)
    )

    val expected = SnowflakeState(input, Nil)

    val result = SnowflakeState.getState(shuffle(input))
    result must beEqualTo(expected)
  }

  def e2 = {
    val initTime = 1502357136000L
    val input = List(
      LoadedRunId("enriched/archived/run-01", new DateTime(initTime + 1000), new DateTime(1502368136000L + 3000), List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"), s3("s3://transformer-output/run-01/"), new DateTime(1502368136000L + 5000), "transformer-2", "loader-1"),
      LoadedRunId("enriched/archived/run-02", new DateTime(initTime + 2000), new DateTime(1502368136000L + 5000), List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"), s3("s3://transformer-output/run-02/"), new DateTime(1502368136000L + 7000), "transformer-2", "loader-1"),
      LoadedRunId("enriched/archived/run-03", new DateTime(initTime + 3000), new DateTime(1502368136000L + 6000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-03/"), new DateTime(1502368136000L + 9000), "transformer-3", "loader-1")
    )

    val expected = SnowflakeState(Nil, input)

    val result = SnowflakeState.getState(shuffle(input))
    result must beEqualTo(expected)
  }

  def e3 = {
    val initTime = 1502357136000L
    val processedInput = List(
      ProcessedRunId("enriched/archived/run-01", new DateTime(initTime + 10000), new DateTime(1502368136000L + 30000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-01/"), "transformer-1", false),
      ProcessedRunId("enriched/archived/run-02", new DateTime(initTime + 20000), new DateTime(1502368136000L + 50000), List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"), s3("s3://transformer-output/run-02/"), "transformer-1", false),
      ProcessedRunId("enriched/archived/run-03", new DateTime(initTime + 30000), new DateTime(1502368136000L + 60000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-03/"), "transformer-1", false)
    )
    val loadedInput = List(
      LoadedRunId("enriched/archived/run-04", new DateTime(initTime + 1000), new DateTime(1502368136000L + 3000), List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"), s3("s3://transformer-output/run-04/"), new DateTime(1502368136000L + 5000), "transformer-1", "loader-2"),
      LoadedRunId("enriched/archived/run-05", new DateTime(initTime + 2000), new DateTime(1502368136000L + 5000), List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"), s3("s3://transformer-output/run-05/"), new DateTime(1502368136000L + 7000), "transformer-1", "loader-2"),
      LoadedRunId("enriched/archived/run-06", new DateTime(initTime + 3000), new DateTime(1502368136000L + 6000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-06/"), new DateTime(1502368136000L + 9000), "transformer-1", "loader-2")
    )

    val freshInput = List(
      FreshRunId("enriched/archived/run-07", new DateTime(initTime + 100000), "transformer-1", false),
      FreshRunId("enriched/archived/run-08", new DateTime(initTime + 200000), "transformer-1", false)
    )

    val expected = SnowflakeState(processedInput, loadedInput)

    val result = SnowflakeState.getState(shuffle(loadedInput ++ processedInput ++ freshInput))
    result must beEqualTo(expected)
  }

  def e4 = {
    val initTime = 1502357136000L
    val loadedInput = List(
      LoadedRunId("enriched/archived/run-01", new DateTime(initTime + 1000), new DateTime(1502368136000L + 3000), List("unstruct_event_com_acme_event_1", "context_com_acme_context_2"), s3("s3://transformer-output/run-01/"), new DateTime(1502368136000L + 5000), "transformer-1", "loader-1"),
      LoadedRunId("enriched/archived/run-02", new DateTime(initTime + 2000), new DateTime(1502368136000L + 5000), List("unstruct_event_com_acme_event_3", "context_com_acme_context_1"), s3("s3://transformer-output/run-02/"), new DateTime(1502368136000L + 7000), "transformer-1", "loader-1"),
      LoadedRunId("enriched/archived/run-03", new DateTime(initTime + 3000), new DateTime(1502368136000L + 6000), List("context_com_acme_context_4"), s3("s3://transformer-output/run-03/"), new DateTime(1502368136000L + 9000), "transformer-1", "loader-1")
    )
    val processedInput = List(
      ProcessedRunId("enriched/archived/run-04", new DateTime(initTime + 10000), new DateTime(1502368136000L + 30000), List("context_com_acme_context_6"), s3("s3://transformer-output/run-04/"), "transformer-1", false),
      ProcessedRunId("enriched/archived/run-05", new DateTime(initTime + 20000), new DateTime(1502368136000L + 50000), List("unstruct_event_com_acme_event_7", "context_com_acme_context_1"), s3("s3://transformer-output/run-05/"), "transformer-1", false),
      ProcessedRunId("enriched/archived/run-06", new DateTime(initTime + 30000), new DateTime(1502368136000L + 60000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-06/"), "transformer-1", false),
      ProcessedRunId("enriched/archived/run-07", new DateTime(initTime + 30000), new DateTime(1502368136000L + 60000), List("context_com_acme_context_10", "context_com_acme_context_11"), s3("s3://transformer-output/run-07/"), "transformer-1", false)
    )
    val expected = List(
      FolderToLoad(
        ProcessedRunId("enriched/archived/run-04", new DateTime(initTime + 10000), new DateTime(1502368136000L + 30000), List("context_com_acme_context_6"), s3("s3://transformer-output/run-04/"), "transformer-1", false),
        Set("context_com_acme_context_6")
      ),
      FolderToLoad(
        ProcessedRunId("enriched/archived/run-05", new DateTime(initTime + 20000), new DateTime(1502368136000L + 50000), List("unstruct_event_com_acme_event_7", "context_com_acme_context_1"), s3("s3://transformer-output/run-05/"), "transformer-1", false),
        Set("unstruct_event_com_acme_event_7")
      ),
      FolderToLoad(
        ProcessedRunId("enriched/archived/run-06", new DateTime(initTime + 30000), new DateTime(1502368136000L + 60000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-06/"), "transformer-1", false),
        Set.empty
      ),
      FolderToLoad(
        ProcessedRunId("enriched/archived/run-07", new DateTime(initTime + 30000), new DateTime(1502368136000L + 60000), List("context_com_acme_context_10", "context_com_acme_context_11"), s3("s3://transformer-output/run-07/"), "transformer-1", false),
        Set("context_com_acme_context_10", "context_com_acme_context_11")
      )
    )

    val result = SnowflakeState(processedInput, loadedInput).foldersToLoad

    result must beEqualTo(expected)
  }
}
