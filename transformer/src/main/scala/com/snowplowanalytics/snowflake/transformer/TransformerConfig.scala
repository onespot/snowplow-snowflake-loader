/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.transformer

import cats.implicits._
import cats.data.Validated

import com.snowplowanalytics.snowflake.core.Temporary
import com.snowplowanalytics.snowflake.core.Config.S3Folder
import com.snowplowanalytics.snowflake.generated.ProjectMetadata

/** End Spark driver config parsed from CLI */
case class TransformerConfig(
  enrichedInput: S3Folder,
  enrichedOutput: S3Folder,
  awsAccessKey: String,
  awsSecretKey: String,
  manifestTable: String)

object TransformerConfig {

  /**
   * Raw CLI configuration used to extract options from command line
   * Created solely for private `rawCliConfig` value and can contain
   * incorrect state that should be handled by `transform` function
   */
  private case class RawConfig(
    enrichedInput: String,
    enrichedOutput: String,
    awsAccessKey: String,
    awsSecretKey: String,
    manifestTable: String)

  /**
   * Starting raw value, required by `parser`
   */
  private val rawCliConfig = RawConfig("", "", "", "", "")

  private val parser = new scopt.OptionParser[RawConfig](Temporary.TransformerName) {
    head(Temporary.TransformerName, ProjectMetadata.version)

    opt[String]('i', "input")
      .required()
      .valueName("s3path")
      .action((x, c) => c.copy(enrichedInput = x))
      .text("Enriched events archive")

    opt[String]('o', "output")
      .required()
      .valueName("s3path")
      .action((x, c) => c.copy(enrichedOutput = x))
      .text("Output folder for Snowflake-ready data")

    opt[String]("aws-access-key-id")
      .required()
      .valueName("key")
      .action((x, c) => c.copy(awsAccessKey = x))
      .text("AWS Access Key Id")

    opt[String]("aws-secret-access-key")
      .required()
      .valueName("key")
      .action((x, c) => c.copy(awsSecretKey = x))
      .text("AWS Secret Access Key")

    opt[String]("manifest-table")
      .required()
      .valueName("table-name")
      .action((x, c) => c.copy(manifestTable = x))
      .text("AWS DynamoDB table to store process manifests")

    help("help").text("prints this usage text")
  }

  /**
   * Check that raw config contains valid state
   */
  def transform(raw: RawConfig): Either[String, TransformerConfig] = {
    (S3Folder.parse(raw.enrichedInput).toValidatedNel |@| S3Folder.parse(raw.enrichedOutput).toValidatedNel).map {
      (input: String, output: String) => TransformerConfig(input, output, raw.awsAccessKey, raw.awsSecretKey, raw.manifestTable)
    } match {
      case Validated.Valid(c) => Right(c)
      case Validated.Invalid(errors) => Left(errors.toList.mkString(", "))
    }
  }

  def parse(args: Array[String]): Option[Either[String, TransformerConfig]] =
    parser.parse(args, rawCliConfig).map(transform)
}
