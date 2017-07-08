/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader

import cats.implicits._

import java.sql.Connection

import ddl._

/** Module containing functions to setup Snowflake table for Enriched events */
object Initializer {

  /** Run setup process */
  def run(config: LoaderConfig.SetupConfig): Unit = {
    val connection = Database.getConnection(config)
    createSchema(connection, config.snowflakeSchema)
    createTable(connection, config.snowflakeSchema)
    createWarehouse(connection, config.snowflakeWarehouse)
    createFileType(connection)
    createStage(connection, config.snowflakeStage, config.stageUrl, config.snowflakeSchema)
  }

  def createSchema(connection: Connection, schemaName: String): Unit = {
    val statement = connection.createStatement()
    val rs = statement.executeQuery(CreateSchema(schemaName).show)
    while (rs.next()) {
      println(rs.getString("status"))
    }
    statement.close()
  }

  def createTable(connection: Connection, schemaName: String): Unit = {
    val statement = connection.createStatement()
    val rs = statement.executeQuery(AtomicDef.getTable(schemaName).show)
    while (rs.next()) {
      println(rs.getString("status"))
    }
    statement.close()
  }

  def createFileType(connection: Connection): Unit = {
    val ddl: CreateFileFormat =
      CreateFileFormat.CreateCsvFormat(Defaults.FileFormat, Some("\n"), None)
    val statement = connection.createStatement()
    val rs = statement.executeQuery(ddl.show)
    while (rs.next()) {
      println(rs.getString("status"))
    }
    statement.close()
  }

  def createWarehouse(connection: Connection, warehouseName: String): Unit = {
    val ddl = CreateWarehouse(warehouseName, size = Some(CreateWarehouse.XSmall))
    val statement = connection.createStatement()
    val rs = statement.executeQuery(ddl.show)
    while (rs.next()) {
      println(rs.getString("status"))
    }
    statement.close()
  }

  def createStage(connection: Connection, stageName: String, stagePath: String, schemaName: String): Unit = {
    val ddl = CreateStage(stageName, stagePath, Defaults.FileFormat, Some(schemaName))
    val statement = connection.createStatement()
    val rs = statement.executeQuery(ddl.show)
    while (rs.next()) {
      println(rs.getString("status"))
    }
    statement.close()
  }
}
