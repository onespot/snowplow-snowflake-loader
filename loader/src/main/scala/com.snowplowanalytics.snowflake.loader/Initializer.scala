/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader

import ast._
import connection.Jdbc

/** Module containing functions to setup Snowflake table for Enriched events */
object Initializer {

  /** Run setup process */
  def run(config: LoaderConfig.SetupConfig): Unit = {
    val connection = Jdbc.getConnection(config)

    Jdbc.executeAndOutput(connection, CreateSchema(config.snowflakeSchema))
    Jdbc.executeAndOutput(connection, AtomicDef.getTable(config.snowflakeSchema))
    Jdbc.executeAndOutput(connection, CreateWarehouse(config.snowflakeWarehouse, size = Some(CreateWarehouse.XSmall)))
    Jdbc.executeAndOutput(connection, CreateFileFormat.CreateJsonFormat(Defaults.FileFormat))
    Jdbc.executeAndOutput(connection, CreateStage(
      config.snowflakeStage, config.stageUrl, Defaults.FileFormat, config.snowflakeSchema))

    connection.close()
  }
}
