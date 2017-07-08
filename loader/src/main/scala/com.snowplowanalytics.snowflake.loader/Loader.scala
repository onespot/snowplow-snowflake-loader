/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader

import scala.util.control.NonFatal
import java.sql.Connection

import cats.implicits._
import com.snowplowanalytics.snowflake.core.{ProcessManifest, ProcessedRunId}
import com.snowplowanalytics.snowflake.loader.LoaderConfig.LoadConfig
import ddl.AtomicDef.columns
import ddl.Defaults

object Loader {

  /** Add new column with VARIANT type to events table */
  def addShredTypes(connection: Connection, schemaName: String, columnName: String) = {
    val statement = connection.createStatement()
    val expr = s"ALTER TABLE $schemaName.${Defaults.Table} ADD COLUMN $columnName VARIANT;"
    try {
      statement.execute(expr)
    } catch {
      case e: net.snowflake.client.jdbc.SnowflakeSQLException =>
        System.err.println(e.getMessage)
        System.err.println("Aborting due invalid manifest state")
        sys.exit(1)
    } finally {
      statement.close()
    }
    println(s"New column [$columnName] has been added")
  }

  /** Drop column */
  def dropShredTypes(connection: Connection, schemaName: String, columnName: String) = {
    val statement = connection.createStatement()
    val expr = s"ALTER TABLE $schemaName.${Defaults.Table} DROP COLUMN $columnName;"
    statement.execute(expr)
    println(s"New column [$columnName] has been added")
    statement.close()
  }

  /**
    * Execute loading statement for processed run id
    * @param connection JDBC connection
    * @param folder run id, extracted from manifest; processed, but not yet loaded
    * @param config load configuration
    */
  def loadFolder(connection: Connection, config: LoadConfig, folder: ProcessedRunId): Unit = {
    val runId = folder.runId.split("/").last
    val tableColumns = columns.map(_.name) ++ folder.shredTypes
    val columnPaths = tableColumns.map("parse_json($1):" + _).mkString(", ")

    val copyStatement =
      s"""
        |COPY INTO ${config.snowflakeSchema}.${Defaults.Table}(${tableColumns.mkString(", ")})
        |  FROM (SELECT $columnPaths FROM @${config.snowflakeSchema}.${config.snowflakeStage}/$runId)
        |  CREDENTIALS = (AWS_KEY_ID = '${config.awsAccessKey}' AWS_SECRET_KEY = '${config.awsSecretKey}')
        |  FILE_FORMAT = (FORMAT_NAME = '${config.snowflakeSchema}.${Defaults.FileFormat}');
      """.stripMargin

    val statement = connection.createStatement()
    statement.execute(copyStatement)
    println(s"Folder [$runId] from stage [${config.snowflakeStage}] has been loaded")
    statement.close()
  }

  /** Scan state from processing manifest, extract not loaded folders and lot each of them */
  def run(config: LoaderConfig.LoadConfig): Unit = {
    val connection = Database.getConnection(config)
    val dynamoDb = ProcessManifest.getDynamoDb(config.awsAccessKey, config.awsSecretKey)
    val state = ProcessManifest.scan(dynamoDb, config.manifestTable).map(SnowflakeState.getState) match {
      case Right(s) => s
      case Left(error) =>
        println(error)
        sys.exit(1)
    }

    val useWarehouse = connection.createStatement()
    useWarehouse.execute(s"USE WAREHOUSE ${config.snowflakeWarehouse};")
    useWarehouse.close()

    try {
      val resumeWarehouse = connection.createStatement()
      resumeWarehouse.execute(s"ALTER WAREHOUSE ${config.snowflakeWarehouse} RESUME;")
      resumeWarehouse.close()
      println(s"Warehouse ${config.snowflakeWarehouse} resumed")
    } catch {
      case _: net.snowflake.client.jdbc.SnowflakeSQLException =>
        println(s"Warehouse ${config.snowflakeWarehouse} already resumed")
    }

    state.foldersToLoad.foreach { folder =>
      folder.newColumns.foreach(addShredTypes(connection, config.snowflakeSchema, _))
      try {
        loadFolder(connection, config, folder.folderToLoad)
      } catch {
        case NonFatal(e) =>
          val message = if (folder.newColumns.isEmpty)
            s"Error during ${folder.folderToLoad.runId} load. No new columns were added, safe to rerun.\n${e.getMessage}"
          else
            s"Error during ${folder.folderToLoad.runId} load. Following columns need to be dropped: ${folder.newColumns.mkString(", ")}.\n${e.getMessage}"
          System.err.println(message)
          sys.exit(1)
      }
      ProcessManifest.markLoaded(dynamoDb, config.manifestTable, folder.folderToLoad.runId)
    }
  }
}
