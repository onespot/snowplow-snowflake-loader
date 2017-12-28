/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
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
