/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader

import com.snowplowanalytics.snowflake.core.RunId
import com.snowplowanalytics.snowflake.core.RunId._
import com.snowplowanalytics.snowflake.loader.SnowflakeState._

import org.joda.time.DateTime

/**
  * Class representing current consistent state snapshot of Snowplow Snowflake Data.
  * Folders (run ids) refer to original archived folders in `enriched.archive`,
  * not folders produced by transformer
  * @param processed ordered list of processed folders
  * @param loaded ordered list of successfully loaded folders
  */
case class SnowflakeState(processed: List[ProcessedRunId], loaded: List[LoadedRunId]) {

  /** Columns that were already added by the time of state snapshot */
  val existingColumns = loaded.flatMap(_.shredTypes).toSet

  private val empty: (List[FolderToLoad], Set[String]) =
    (List.empty, existingColumns)

  /**
    * Calculate list of folders that have to be loaded in particular order,
    * with each having set of columns that appeared **first** in this folder
    */
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

  /**
    * Folder that was processed by Transformer and ready to be loaded into Snowflake,
    * and containing set of columns that first appeared in this folder (according to
    * manifest state)
    * @param folderToLoad reference to folder processed by Transformer
    * @param newColumns set of columns this processed folder brings
    */
  case class FolderToLoad(folderToLoad: ProcessedRunId, newColumns: Set[String])

  implicit def dateTimeOrdering: Ordering[DateTime] =
    Ordering.fromLessThan(_ isBefore _)

  /** Extract state from full Snowflake manifest state */
  def getState(runIds: List[RunId]): SnowflakeState = {
    val sortedRunIds = runIds.sortBy(_.addedAt).filterNot(_.toSkip)       // TODO: DynamoDB query
    val processed = sortedRunIds.collect { case x: ProcessedRunId => x }  // next
    val loaded = sortedRunIds.collect { case x: LoadedRunId => x }        // done
    SnowflakeState(processed, loaded)
  }
}
