/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader

import ast._
import com.snowplowanalytics.snowflake.core.Config
import connection.Jdbc

/** Module containing functions to setup Snowflake table for Enriched events */
object Initializer {

  /** Run setup process */
  def run(config: Config): Unit = {
    val connection = Jdbc.getConnection(config)

    val staticCredentials = for {
      accessKey <- config.accessKeyId
      secretKey <- config.secretAccessKey
    } yield Common.AwsCreds(accessKey, secretKey)

    Jdbc.executeAndOutput(connection, CreateSchema(config.schema))
    Jdbc.executeAndOutput(connection, AtomicDef.getTable(config.schema))
    Jdbc.executeAndOutput(connection, CreateWarehouse(config.warehouse, size = Some(CreateWarehouse.XSmall)))
    Jdbc.executeAndOutput(connection, CreateFileFormat.CreateJsonFormat(Defaults.FileFormat))
    Jdbc.executeAndOutput(connection, CreateStage(
      config.stage, config.stageUrl, Defaults.FileFormat, config.schema, staticCredentials))

    connection.close()
  }
}
