/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader

import com.snowplowanalytics.snowflake.loader.ast._

/** Module containing functions to setup Snowflake table for Enriched events */
object Initializer {

  /** Run setup process */
  def run(config: LoaderConfig.SetupConfig): Unit = {
    val connection = Database.getConnection(config)

    Database.executeAndOutput(connection, CreateSchema(config.snowflakeSchema))
    Database.executeAndOutput(connection, AtomicDef.getTable(config.snowflakeSchema))
    Database.executeAndOutput(connection, CreateWarehouse(config.snowflakeWarehouse, size = Some(CreateWarehouse.XSmall)))
    Database.executeAndOutput(connection, CreateFileFormat.CreateCsvFormat(Defaults.FileFormat, Some("\n"), None))
    Database.executeAndOutput(connection, CreateStage(
      config.snowflakeStage, config.stageUrl, Defaults.FileFormat, config.snowflakeSchema))

    connection.close()
  }
}
