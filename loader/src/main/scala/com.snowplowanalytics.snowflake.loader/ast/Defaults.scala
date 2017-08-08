/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader.ast

object Defaults {
  val FileFormat = "snowplow_enriched"
  val Schema = "atomic"
  val Table = "events"
  val TempTableColumn = "enriched_data"
}
