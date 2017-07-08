/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.transformer

import com.snowplowanalytics.snowplow.analytics.scalasdk.json.{ Data, EventTransformer }

object Transformer {

  /**
    * Transform TSV to pair of shredded keys and enriched event in JSON format
    * @param line enriched event TSV
    * @return pair of set with column names and JSON string, ready to be saved
    */
  def transform(line: String): (Set[String], String) = {
    EventTransformer.transformWithInventory(line) match {
      case Right(eventWithInventory) =>
        val shredTypes = eventWithInventory.inventory.map(item => Data.fixSchema(item.shredProperty, item.igluUri))
        (shredTypes, eventWithInventory.event)
      case Left(e) =>
        throw new RuntimeException(e.mkString("\n"))
    }
  }
}
