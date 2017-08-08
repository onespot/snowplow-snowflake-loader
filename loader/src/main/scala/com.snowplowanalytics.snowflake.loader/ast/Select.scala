/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader.ast

import Select._

/**
  * AST for SELECT statement
  * E.g. SELECT raw:app_id::VARCHAR, event_id:event_id::VARCHAR FROM temp_table
  * @param columns list of columns, casted to specific type
  * @param table source table name
  */
case class Select(columns: List[CastedColumn], schema: String, table: String)

object Select {
  case class CastedColumn(originColumn: String, columnName: String, datatype: SnowflakeDatatype)
}
