/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader

import com.snowplowanalytics.snowflake.core.Config.S3Folder
import com.snowplowanalytics.snowflake.generated.ProjectMetadata
import com.snowplowanalytics.snowflake.loader.ast.Defaults

/** Common loader configuration interface */
sealed trait LoaderConfig {
  def awsAccessKey: String
  def awsSecretKey: String
  def manifestTable: String

  def snowflakeRegion: String
  def snowflakeStage: String
  def snowflakeUser: String
  def snowflakePassword: String
  def snowflakeAccount: String
  def snowflakeWarehouse: String
  def snowflakeDb: String
  def snowflakeSchema: String
}

object LoaderConfig {

  /** Configuration for `load` subcommand */
  case class LoadConfig(
    awsAccessKey: String,
    awsSecretKey: String,
    awsRegion: String,
    manifestTable: String,

    snowflakeRegion: String,
    snowflakeStage: String,
    snowflakeUser: String,
    snowflakePassword: String,
    snowflakeAccount: String,
    snowflakeWarehouse: String,
    snowflakeDb: String,
    snowflakeSchema: String,
    dryRun: Boolean = false
  ) extends LoaderConfig

  /** Configuration for `setup` subcommand */
  case class SetupConfig(
    awsAccessKey: String,
    awsSecretKey: String,
    manifestTable: String,

    stageUrl: S3Folder,

    snowflakeRegion: String,
    snowflakeStage: String,
    snowflakeUser: String,
    snowflakePassword: String,
    snowflakeAccount: String,
    snowflakeWarehouse: String,
    snowflakeDb: String,
    snowflakeSchema: String
  ) extends LoaderConfig


  /**
   * Raw CLI configuration used to extract options from command line
   * Created solely for private `rawCliConfig` value and can contain
   * incorrect state that should be handled by `transform` function
   */
  private case class RawConfig(
    awsAccessKey: String,
    awsSecretKey: String,
    awsRegion: String,
    manifestTable: String,

    stageUrl: String,

    snowflakeRegion: String,
    snowflakeStage: String,
    snowflakeUser: String,
    snowflakePassword: String,
    snowflakeAccount: String,
    snowflakeWarehouse: String,
    snowflakeDb: String,
    snowflakeSchema: Option[String],

    dryRun: Boolean,

    command: String)

  /** Parse and validate Snowflake Loader configuration out of CLI args */
  def parse(args: Array[String]): Option[Either[String, LoaderConfig]] =
    parser.parse(args, rawCliConfig).map(transform)

  /** Validate raw configuration into consistent end configuration */
  def transform(rawConfig: RawConfig): Either[String, LoaderConfig] = rawConfig.command match {
    case "setup" =>
      S3Folder.parse(rawConfig.stageUrl) match {
        case Right(stageUrl) =>
          if (rawConfig.dryRun)
            Left("setup subcommand does not accept --dry-run")
          else
            Right(LoaderConfig.SetupConfig(
              awsAccessKey = rawConfig.awsAccessKey,
              awsSecretKey = rawConfig.awsSecretKey,
              manifestTable = rawConfig.manifestTable,

              stageUrl = stageUrl,

              snowflakeRegion = rawConfig.snowflakeRegion,
              snowflakeStage = rawConfig.snowflakeStage,
              snowflakeUser = rawConfig.snowflakeUser,
              snowflakePassword = rawConfig.snowflakePassword,
              snowflakeAccount = rawConfig.snowflakeAccount,
              snowflakeWarehouse = rawConfig.snowflakeWarehouse,
              snowflakeDb = rawConfig.snowflakeDb,
              snowflakeSchema = rawConfig.snowflakeSchema.getOrElse(Defaults.Schema)))

        case Left(e) =>
          Left(s"${rawConfig.stageUrl} is invalid S3 stage. " + e)
      }
    case "load" =>
      Right(LoaderConfig.LoadConfig(
        awsAccessKey = rawConfig.awsAccessKey,
        awsSecretKey = rawConfig.awsSecretKey,
        awsRegion = rawConfig.awsRegion,
        manifestTable = rawConfig.manifestTable,

        snowflakeRegion = rawConfig.snowflakeRegion,
        snowflakeStage = rawConfig.snowflakeStage,
        snowflakeUser = rawConfig.snowflakeUser,
        snowflakePassword = rawConfig.snowflakePassword,
        snowflakeAccount = rawConfig.snowflakeAccount,
        snowflakeWarehouse = rawConfig.snowflakeWarehouse,
        snowflakeDb = rawConfig.snowflakeDb,
        snowflakeSchema = rawConfig.snowflakeSchema.getOrElse(Defaults.Schema)))

    case "noop" => Left(s"Either setup or load actions must be provided")
    case command => Left(s"Unknown action $command")
  }

  /**
    * Starting raw value, required by `parser`
    */
  private val rawCliConfig = RawConfig("", "", "", "", "", "", "", "", "", "", "", "", None, false, "noop")

  private val parser = new scopt.OptionParser[RawConfig](ProjectMetadata.name + "-" + ProjectMetadata.version + ".jar") {
    head(ProjectMetadata.name, ProjectMetadata.version)

    cmd("setup")
      .action((_, c) => c.copy(command = "setup"))
      .text("Perform initialization instead of loading")
      .children(
        opt[String]("aws-access-key-id")
          .required()
          .valueName("key")
          .action((x, c) => c.copy(awsAccessKey = x))
          .text("AWS Access Key Id"),

        opt[String]("aws-secret-access-key")
          .required()
          .valueName("key")
          .action((x, c) => c.copy(awsSecretKey = x))
          .text("AWS Secret Access Key"),

        opt[String]("manifest-table")
          .required()
          .valueName("<value>")
          .action((x, c) => c.copy(manifestTable = x))
          .text("AWS DynamoDB table to store process manifests"),

        opt[String]("snowflake-region")
          .required()
          .valueName("region")
          .action((x, c) => c.copy(snowflakeRegion = x))
          .text("AWS Region to connect to Snowflake"),

        opt[String]("stage-url")
          .required()
          .valueName("<s3://...>")
          .action((x, c) => c.copy(stageUrl = x))
          .text("S3 bucket to transformer output (only for setup)"),

        opt[String]("stage-name")
          .required()
          .valueName("<value>")
          .action((x, c) => c.copy(snowflakeStage = x))
          .text("Stage name"),

        opt[String]("user")
          .required()
          .valueName("<value>")
          .action((x, c) => c.copy(snowflakeUser = x))
          .text("JDBC User"),

        opt[String]("password")
          .required()
          .valueName("<value>")
          .action((x, c) => c.copy(snowflakePassword = x))
          .text("JDBC Password"),

        opt[String]("account")
          .required()
          .valueName("<value>")
          .action((x, c) => c.copy(snowflakeAccount = x))
          .text("Account name"),

        opt[String]("warehouse")
          .required()
          .valueName("<value>")
          .action((x, c) => c.copy(snowflakeWarehouse = x))
          .text("Warehouse name"),

        opt[String]("db")
          .required()
          .valueName("<value>")
          .action((x, c) => c.copy(snowflakeDb = x))
          .text("Database name"),

        opt[String]("schema")
          .valueName("<value>")
          .action((x, c) => c.copy(snowflakeSchema = Some(x)))
          .text("Database Schema name")
      )

    cmd("load")
      .action((_, c) => c.copy(command = "load"))
      .text("Load enriched data from S3")
      .children(
        opt[String]("aws-access-key-id")
          .required()
          .valueName("key")
          .action((x, c) => c.copy(awsAccessKey = x))
          .text("AWS Access Key Id"),

        opt[String]("aws-secret-access-key")
          .required()
          .valueName("key")
          .action((x, c) => c.copy(awsSecretKey = x))
          .text("AWS Secret Access Key"),

        opt[String]("aws-region")
          .required()
          .valueName("region")
          .action((x, c) => c.copy(awsRegion = x))
          .text("AWS Region"),

        opt[String]("manifest-table")
          .required()
          .valueName("table-name")
          .action((x, c) => c.copy(manifestTable = x))
          .text("AWS DynamoDB table to store process manifests"),

        opt[String]("snowflake-region")
          .required()
          .valueName("region")
          .action((x, c) => c.copy(snowflakeRegion = x))
          .text("AWS Region to connect to Snowflake"),

        opt[String]("stage-name")
          .required()
          .valueName("name")
          .action((x, c) => c.copy(snowflakeStage = x))
          .text("Stage name"),

        opt[String]("user")
          .required()
          .valueName("<value>")
          .action((x, c) => c.copy(snowflakeUser = x))
          .text("JDBC User"),

        opt[String]("password")
          .required()
          .valueName("<value>")
          .action((x, c) => c.copy(snowflakePassword = x))
          .text("JDBC Password"),

        opt[String]("account")
          .required()
          .valueName("<value>")
          .action((x, c) => c.copy(snowflakeAccount = x))
          .text("Account name"),

        opt[String]("warehouse")
          .required()
          .valueName("<value>")
          .action((x, c) => c.copy(snowflakeWarehouse = x))
          .text("Warehouse name"),

        opt[String]("db")
          .required()
          .valueName("<value>")
          .action((x, c) => c.copy(snowflakeDb = x))
          .text("Database name"),

        opt[String]("schema")
          .valueName("<value>")
          .action((x, c) => c.copy(snowflakeSchema = Some(x)))
          .text("Database Schema name"),

        opt[Unit]("dry-run")
          .action((_, c) => c.copy(dryRun = true))
          .text("Do not perform database actions")
      )

    help("help").text("prints this usage text")
  }
}
