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

import LoaderConfig.LoadConfig
import ddl.AtomicDef.columns
import ddl.{Column, CreateTable, Defaults, SnowflakeDatatype}
import com.snowplowanalytics.snowflake.core.{ProcessManifest, ProcessedRunId}

object Loader {

  /**
    * Execute loading statement for processed run id
    * @param connection JDBC connection
    * @param config load configuration
    * @param folder run id, extracted from manifest; processed, but not yet loaded
    */
  def loadFolder(connection: Connection, config: LoadConfig, folder: ProcessedRunId): Unit = {
    val runId = folder.runId.split("/").last
    val tempTable = getTempTable(runId, config.snowflakeSchema)
    val tableColumns = getColumns(folder.shredTypes)

    loadTempTable(connection, config, tempTable, runId)

    val copyStatement =
      s"""
        |INSERT INTO ${config.snowflakeSchema}.${Defaults.Table}(${tableColumns.map(_._1).mkString(", ")})
        |  SELECT ${tableColumns.map { case (n, t) => s"${Defaults.TempTableColumn}:$n::$t" }.mkString(", ") }
        |  FROM ${tempTable.getFullTableName};
      """.stripMargin

    val statement = connection.createStatement()
    statement.execute(copyStatement)
    println(s"Folder [$runId] from stage [${config.snowflakeStage}] has been loaded")
    statement.close()
  }

  /** Add new column with VARIANT type to events table */
  def addShredType(connection: Connection, schemaName: String, columnName: String) = {
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
    * Build CREATE TABLE statement for temporary table
    * @param runId arbitrary identifier
    * @param dbSchema Snowflake DB Schema
    * @return SQL statement AST
    */
  def getTempTable(runId: String, dbSchema: String): CreateTable = {
    val tempTableName = "snowplow_tmp_" + runId.replace('=', '_').replace('-', '_')
    val enrichedColumn = Column(Defaults.TempTableColumn, SnowflakeDatatype.JsonObject, notNull = true)
    CreateTable(Some(dbSchema), tempTableName, List(enrichedColumn), None, temporary = true)
  }

  /**
    * Get list of pairs with column name and datatype based on canonical columns and new shred types
    * @param shredTypes shred types discovered in particular run id
    * @return pairs of string, ready to be used in statement, e.g. (app_id, VARCHAR(255))
    */
  def getColumns(shredTypes: List[String]): List[(String, String)] = {
    val atomicColumns: List[(String, String)] = columns.map(c => (c.name, c.dataType.show))
    val shredTypeColumns: List[(String, String)] = shredTypes.map(getShredType).sequence match {
      case Right(types) => types.map { case (name, t) => (name, t.show) }
      case Left(e) => throw new RuntimeException(e)
    }
    atomicColumns ++ shredTypeColumns
  }

  /**
    * Create temporary table with single OBJECT column and load data from S3
    * @param connection JDBC connection
    * @param config loader configuration
    * @param tempTableCreateStatement SQL statement AST
    * @param runId directory in stage, where files reside
    */
  def loadTempTable(connection: Connection, config: LoadConfig, tempTableCreateStatement: CreateTable, runId: String): Unit = {
    val tempTableCopyStatement =
      s"""
         |COPY INTO ${tempTableCreateStatement.getFullTableName}(${Defaults.TempTableColumn})
         |  FROM @${config.snowflakeSchema}.${config.snowflakeStage}/$runId
         |  CREDENTIALS = (AWS_KEY_ID = '${config.awsAccessKey}' AWS_SECRET_KEY = '${config.awsSecretKey}')
         |  FILE_FORMAT = (FORMAT_NAME = '${config.snowflakeSchema}.${Defaults.FileFormat}');
       """.stripMargin

    val createStatement = connection.createStatement()
    createStatement.execute(tempTableCreateStatement.show)
    createStatement.close()

    val copyStatement = connection.createStatement()
    copyStatement.execute(tempTableCopyStatement)
    copyStatement.close()
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
      folder.newColumns.foreach(addShredType(connection, config.snowflakeSchema, _))
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

  /** Try to infer ARRAY/OBJECT type, based on column name */
  def getShredType(columnName: String): Either[String, (String, SnowflakeDatatype)] = {
    if (columnName.startsWith("context_") && columnName.last.isDigit) {
      Right(columnName -> SnowflakeDatatype.JsonArray)
    } else if (columnName.startsWith("unstruct_event_") && columnName.last.isDigit) {
      Right(columnName -> SnowflakeDatatype.JsonObject)
    } else {
      Left(s"Column [$columnName] is not a shredded type")
    }
  }
}
