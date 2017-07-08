package com.snowplowanalytics.snowflake.loader.ast

sealed trait AlterWarehouse

object AlterWarehouse {
  case class Resume(warehouse: String) extends AlterWarehouse
}
