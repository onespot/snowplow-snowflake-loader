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

import cats.data.{ Validated, ValidatedNel }
import cats.implicits._

import com.snowplowanalytics.snowflake.core.{ ProcessManifest, RunId }

import ast._
import ast.Insert.InsertQuery
import LoaderConfig.LoadConfig

object Loader {

  /** Check that necessary Snowflake entities are available before actual load */
  def preliminaryChecks(connection: Connection, schemaName: String, stageName: String, warehouseName: String): ValidatedNel[String, Unit] = {
    val schema = if (Database.executeAndCountRows(connection, Show.ShowSchemas(Some(schemaName))) < 1)
      s"Schema $schemaName does not exist".invalidNel
    else ().validNel
    val stage = if (Database.executeAndCountRows(connection, Show.ShowStages(Some(stageName), Some(schemaName))) < 1)
      s"Stage $stageName does not exist".invalidNel
    else ().validNel
    val table = if (Database.executeAndCountRows(connection, Show.ShowTables(Some(Defaults.Table), Some(schemaName))) < 1)
      s"Table ${Defaults.Table} does not exist".invalidNel
    else ().validNel
    val fileFormat = if (Database.executeAndCountRows(connection, Show.ShowFileFormats(Some(Defaults.FileFormat), Some(schemaName))) < 1)
      s"File format ${Defaults.FileFormat} does not exist".invalidNel
    else ().validNel
    val warehouse = if (Database.executeAndCountRows(connection, Show.ShowWarehouses(Some(warehouseName))) < 1)
      s"Warehouse $warehouseName does not exist".invalidNel
    else ().validNel

    (schema, stage, table, fileFormat, warehouse).map5 { (_: Unit, _: Unit, _: Unit, _: Unit, _: Unit) => () }
  }

  /** Scan state from processing manifest, extract not-loaded folders and lot each of them */
  def run(config: LoaderConfig.LoadConfig): Unit = {
    val connection = Database.getConnection(config)
    val dynamoDb = ProcessManifest.getDynamoDb(config.awsAccessKey, config.awsSecretKey, config.awsRegion)
    val state = ProcessManifest.scan(dynamoDb, config.manifestTable).map(SnowflakeState.getState) match {
      case Right(s) => s
      case Left(error) =>
        System.err.println(error)
        sys.exit(1)
    }

    preliminaryChecks(connection, config.snowflakeSchema, config.snowflakeStage, config.snowflakeWarehouse) match {
      case Validated.Valid(()) => println("Preliminary checks passed")
      case Validated.Invalid(errors) =>
        val message = s"Preliminary checks failed. ${errors.toList.mkString(", ")}"
        System.err.println(message)
        sys.exit(1)
    }

    if (state.foldersToLoad.isEmpty) {
      println("Nothing to load, exiting...")
      sys.exit(0)
    } else {
      Database.execute(connection, UseWarehouse(config.snowflakeWarehouse))
      try {
        Database.execute(connection, AlterWarehouse.Resume(config.snowflakeWarehouse))
        println(s"Warehouse ${config.snowflakeWarehouse} resumed")
      } catch {
        case _: net.snowflake.client.jdbc.SnowflakeSQLException =>
          println(s"Warehouse ${config.snowflakeWarehouse} already resumed")
      }

      state.foldersToLoad.foreach { folder =>
        addColumns(connection, config.snowflakeSchema, folder)
        try {
          loadFolder(connection, config, folder.folderToLoad)
        } catch {
          case NonFatal(e) =>
            val message = if (folder.newColumns.isEmpty)
              s"Error during ${folder.folderToLoad.runId} load. Details: \n${e.getMessage}\n" +
                s"No new columns were added, safe to rerun."
            else
              s"Error during ${folder.folderToLoad.runId} load. Details: \n${e.getMessage}\n" +
                s"Following columns were added during load-preparation and must be dropped to restore state: ${folder.newColumns.mkString(", ")}"
            System.err.println(message)
            sys.exit(1)
        }
        ProcessManifest.markLoaded(dynamoDb, config.manifestTable, folder.folderToLoad.runId)
      }
    }
  }

  /** Create INSERT statement to load Processed Run Id */
  def getInsertStatement(config: LoadConfig, folder: RunId.ProcessedRunId): Insert = {
    val tableColumns = getColumns(folder.shredTypes)
    val castedColumns = tableColumns.map { case (name, dataType) => Select.CastedColumn(Defaults.TempTableColumn, name, dataType) }
    val tempTable = getTempTable(folder.runIdFolder, config.snowflakeSchema)
    val source = Select(castedColumns, tempTable.schema, tempTable.name)
    InsertQuery(config.snowflakeSchema, Defaults.Table, tableColumns.map(_._1), source)
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
    CreateTable(dbSchema, tempTableName, List(enrichedColumn), None, temporary = true)
  }

  /**
    * Get list of pairs with column name and datatype based on canonical columns and new shred types
    * @param shredTypes shred types discovered in particular run id
    * @return pairs of string, ready to be used in statement, e.g. (app_id, VARCHAR(255))
    */
  def getColumns(shredTypes: List[String]): List[(String, SnowflakeDatatype)] = {
    val atomicColumns = AtomicDef.columns.map(c => (c.name, c.dataType))
    val shredTypeColumns = shredTypes.map(getShredType).sequence match {
      case Right(types) => types
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
    val tempTableCopyStatement = CopyInto(
      tempTableCreateStatement.schema,
      tempTableCreateStatement.name,
      List(Defaults.TempTableColumn),
      CopyInto.From(config.snowflakeSchema, config.snowflakeStage, runId),
      CopyInto.AwsCreds(config.awsAccessKey, config.awsSecretKey),
      CopyInto.FileFormat(config.snowflakeSchema, Defaults.FileFormat))

    Database.execute(connection, tempTableCreateStatement)
    Database.execute(connection, tempTableCopyStatement)
  }

  /** Try to infer ARRAY/OBJECT type, based on column name */
  def getShredType(columnName: String): Either[String, (String, SnowflakeDatatype)] = {
    if (columnName.startsWith("contexts_") && columnName.last.isDigit) {
      Right(columnName -> SnowflakeDatatype.JsonArray)
    } else if (columnName.startsWith("unstruct_event_") && columnName.last.isDigit) {
      Right(columnName -> SnowflakeDatatype.JsonObject)
    } else {
      Left(s"Column [$columnName] is not a shredded type")
    }
  }

  // Actual DB IO actions

  /**
    * Execute loading statement for processed run id
    * @param connection JDBC connection
    * @param config load configuration
    * @param folder run id, extracted from manifest; processed, but not yet loaded
    */
  private def loadFolder(connection: Connection, config: LoadConfig, folder: RunId.ProcessedRunId): Unit = {
    val runId = folder.runIdFolder
    val tempTable = getTempTable(runId, config.snowflakeSchema)
    loadTempTable(connection, config, tempTable, runId)

    val loadStatement = getInsertStatement(config, folder)
    Database.execute(connection, loadStatement)
    println(s"Folder [$runId] from stage [${config.snowflakeStage}] has been loaded")
  }

  /** Add new column with VARIANT type to events table */
  private def addShredType(connection: Connection, schemaName: String, columnName: String): Unit = {
    try {
      val shredType = getShredType(columnName) match {
        case Right((_, datatype)) => datatype
        case Left(error) =>
          System.err.println(error)
          sys.exit(1)
      }
      Database.execute(connection, AlterTable.AddColumn(schemaName, Defaults.Table, columnName, shredType))
    } catch {
      case e: net.snowflake.client.jdbc.SnowflakeSQLException =>
        System.err.println(e.getMessage)
        System.err.println("Aborting due invalid manifest state")
        sys.exit(1)
    }
    println(s"New column [$columnName] has been added")
  }

  /**
    * Add columns for shred types that were encountered for particular folder to events table (in alphabetical order)
    * Print human-readable error and exit in case of error
    * @param connection JDBC connection
    * @param schema Snowflake Schema for events table
    * @param folder folder parsed from manifest
    */
  private def addColumns(connection: Connection, schema: String, folder: SnowflakeState.FolderToLoad): Unit = {
    val columns = folder.newColumns.toList.sorted

    val _ = columns.foldLeft(List.empty[String]) { (created, current) =>
      try {
        addShredType(connection, schema, current)
        current :: created
      } catch {
        case NonFatal(e) =>
          val message = s"Error during ${folder.folderToLoad.runId} load. Details: \n${e.getMessage}\n" +
            s"Following columns were added during load-preparation and must be dropped to restore state: ${folder.newColumns.mkString(", ")}"
          System.err.println(message)
          sys.exit(1)
      }
    }
    ()
  }
}
