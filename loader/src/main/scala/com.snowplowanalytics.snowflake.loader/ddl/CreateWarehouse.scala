/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader.ddl

import CreateWarehouse._

case class CreateWarehouse(name: String, size: Option[Size])

object CreateWarehouse {

  val DefaultSize: Size = XSmall

  sealed trait Size
  case object XSmall extends Size
  case object Small extends Size
  case object Medium extends Size
  case object Large extends Size
  case object XLarge extends Size
  case object XxLarge extends Size
  case object XxxLarge extends Size
}
