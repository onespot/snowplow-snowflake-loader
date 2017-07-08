/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader

import com.snowplowanalytics.snowflake.core.{LoadedRunId, ProcessedRunId, RunId}
import com.snowplowanalytics.snowflake.loader.SnowflakeState._

import org.joda.time.DateTime

case class SnowflakeState(processed: List[ProcessedRunId], loaded: List[LoadedRunId]) {

  val existingColumns = loaded.flatMap(_.shredTypes).toSet

  private val empty: (List[FolderToLoad], Set[String]) =
    (List.empty, existingColumns)

  def foldersToLoad: List[FolderToLoad] = {
    val (toLoad, _) = processed.foldLeft(empty) { case ((loadStates, shredTypes), cur) =>
      val newColumns = cur.shredTypes.toSet -- shredTypes
      val loadState = FolderToLoad(cur, newColumns)
      (loadState :: loadStates, newColumns ++ shredTypes)
    }
    toLoad.reverse
  }
}

object SnowflakeState {

  case class FolderToLoad(folderToLoad: ProcessedRunId, newColumns: Set[String])

  implicit def dateTimeOrdering: Ordering[DateTime] =
    Ordering.fromLessThan(_ isBefore _)

  def getState(runIds: List[RunId]): SnowflakeState = {
    val sortedRunIds = runIds.sortBy(_.startedAt)
    val processed = sortedRunIds.collect { case x: ProcessedRunId => x }  // next
    val loaded = sortedRunIds.collect { case x: LoadedRunId => x }        // done
    SnowflakeState(processed, loaded)
  }
}
