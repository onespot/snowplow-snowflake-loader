/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowflake.core

import scala.io.Source
import scala.util.control.NonFatal

import java.io.File
import java.util.Base64

import cats.implicits._

import org.json4s.JsonAST.{JInt, JNull}
import org.json4s.{CustomSerializer, JObject, JString, JValue, MappingException}
import org.json4s.jackson.JsonMethods.parse

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.validation.ValidatableJValue.validate
import com.snowplowanalytics.snowflake.generated.ProjectMetadata

/** Common loader configuration interface, extracted from configuration file */
case class Config(
  accessKeyId: Option[String],
  secretAccessKey: Option[String],
  awsRegion: String,
  manifest: String,

  snowflakeRegion: String,
  stage: String,
  stageUrl: Config.S3Folder,
  input: Config.S3Folder,
  username: String,
  password: String,
  account: String,
  warehouse: String,
  database: String,
  schema: String
)

object Config {

  implicit val formats = org.json4s.DefaultFormats + s3FolderSerializer

  sealed trait Command
  case object LoadCommand extends Command
  case object SetupCommand extends Command

  case class RawCliLoader(command: String, loaderConfig: String, resolver: String, dryRun: Boolean, base64: Boolean)
  case class CliLoaderConfiguration(command: Command, loaderConfig: Config, dryRun: Boolean)

  case class RawCliTransformer(loaderConfig: String, resolver: String)
  case class CliTransformerConfiguration(loaderConfig: Config)

  /** Parse and validate Snowflake Loader configuration out of CLI args */
  def parseLoaderCli(args: Array[String]): Option[Either[String, CliLoaderConfiguration]] =
    loaderCliParser.parse(args, rawCliLoader).map(validateLoaderCli)

  def parseTransformerCli(args: Array[String]): Option[Either[String, CliTransformerConfiguration]] =
    transformerCliParser.parse(args, rawCliTransformer).map(validateTransformerCli)

  /** Validate raw loader's CLI configuration */
  def validateLoaderCli(rawConfig: RawCliLoader): Either[String, CliLoaderConfiguration] = {
    for {
      command <- getSubcommand(rawConfig.command)
      resolverConfig <- parseJsonFile(rawConfig.resolver, rawConfig.base64)
      resolver <- Resolver.parse(resolverConfig).toEither.leftMap { x => s"Resolver parse error: ${x.list.mkString(", ")}" }
      json <- parseJsonFile(rawConfig.loaderConfig, rawConfig.base64)
      validConfig <- validate(json, true)(resolver).toEither.leftMap { x => s"Validation error: ${x.list.mkString(", ")}" }
      config <- extract(validConfig)
    } yield CliLoaderConfiguration(command, config, rawConfig.dryRun)
  }

  /** Validate raw transformer's CLI configuration  */
  def validateTransformerCli(rawConfig: RawCliTransformer): Either[String, CliTransformerConfiguration] = {
    for {
      resolverConfig <- parseJsonFile(rawConfig.resolver, base64 = true)
      resolver <- Resolver.parse(resolverConfig).toEither.leftMap { x => s"Resolver parse error: ${x.list.mkString(", ")}" }
      json <- parseJsonFile(rawConfig.loaderConfig, true)
      validConfig <- validate(json, true)(resolver).toEither.leftMap { x => s"Validation error: ${x.list.mkString(", ")}" }
      config <- extract(validConfig)
    } yield CliTransformerConfiguration(config)
  }


  /** Starting raw value, required by `parser` */
  private val rawCliLoader = RawCliLoader("noop", "", "", false, false)
  private val loaderCliParser = new scopt.OptionParser[RawCliLoader](ProjectMetadata.name + "-" + ProjectMetadata.version + ".jar") {
    head(ProjectMetadata.name, ProjectMetadata.version)

    cmd("setup")
      .action((_, c) => c.copy(command = "setup"))
      .text("Perform initialization instead of loading")

    cmd("load")
      .action((_, c) => c.copy(command = "load"))
      .text("Load enriched data from S3")
      .children(
        opt[Unit]("dry-run")
          .action((_, c) => c.copy(dryRun = true))
          .text("Do not perform database actions")
      )

    opt[String]("config")
      .required()
      .valueName("target.json")
      .action((x, c) => c.copy(loaderConfig = x))
      .text("Snowplow Snowflake Loader configuration JSON")

    opt[String]("resolver")
      .required()
      .valueName("resolver.json")
      .action((x, c) => c.copy(resolver = x))
      .text("Iglu Resolver configuration JSON")

    opt[Unit]("base64")
      .action((_, c) => c.copy(base64 = true))
      .text("Whether configuration and resolver are base64-encoded strings instead of paths")

    help("help").text("prints this usage text")
  }

  private val rawCliTransformer = RawCliTransformer("", "")
  private val transformerCliParser = new scopt.OptionParser[RawCliTransformer](ProjectMetadata.name + "-" + ProjectMetadata.version + ".jar") {
    head(ProjectMetadata.name, ProjectMetadata.version)

    opt[String]("config")
      .required()
      .valueName("target.json")
      .action((x, c) => c.copy(loaderConfig = x))
      .text("Base64-encoded Snowplow Snowflake Loader configuration JSON")

    opt[String]("resolver")
      .required()
      .valueName("resolver.json")
      .action((x, c) => c.copy(resolver = x))
      .text("Base64-encoded Iglu Resolver configuration JSON")

    help("help").text("prints this usage text")
  }

  // Common functionality

  /** Parse JSON either container or local file or base64-encoded string */
  def parseJsonFile(ref: String, base64: Boolean): Either[String, JValue] = {
    for {
      content <- try {
        if (base64) Right(new String(Base64.getDecoder.decode(ref)))
        else Right(Source.fromFile(new File(ref)).mkString)
      } catch {
        case NonFatal(e) => Left(e.getMessage)
      }
      json <- try {
        Right(parse(content))
      } catch {
        case NonFatal(e) => Left(e.getMessage)
      }
    } yield json
  }

  /** Extract LoaderConfig class from JSON */
  private def extract(json: JValue): Either[String, Config] = {
    try {
      Right(json.extract[Config])
    } catch {
      case NonFatal(e) => Left(s"Cannot extract configuration: ${e.getMessage}")
    }
  }

  private def getSubcommand(subcommand: String): Either[String, Command] = subcommand match {
    case "setup" => Right(SetupCommand)
    case "load" => Right(LoadCommand)
    case "noop" => Left(s"Either setup or load actions must be provided")
    case command => Left(s"Unknown action $command")
  }

  /**
    * Extract `s3://path/run=YYYY-MM-dd-HH-mm-ss/atomic-events` part from
    * Set of prefixes that can be used in config.yml
    * In the end it won't affect how S3 is accessed
    */
  val supportedPrefixes = Set("s3", "s3n", "s3a")

  /** Weak newtype replacement to mark string prefixed with s3:// and ended with trailing slash */
  object S3Folder {
    def parse(s: String): Either[String, S3Folder] = s match {
      case _ if !correctlyPrefixed(s) => Left(s"Bucket name [$s] must start with s3:// prefix")
      case _ if s.length > 1024       => Left("Key length cannot be more than 1024 symbols")
      case _                          => Right(appendTrailingSlash(fixPrefix(s)))
    }

    def coerce(s: String) = parse(s) match {
      case Right(f) => f
      case Left(error) => throw new IllegalArgumentException(error)
    }
  }

  class S3Folder(val path: String) extends AnyVal {
    /** Split valid S3 folder path to bucket and path */
    def splitS3Folder: (String, String) =
      path.stripPrefix("s3://").split("/").toList match {
        case head :: Nil => (head, "/")
        case head :: tail => (head, tail.mkString("/") + "/")
        case Nil => throw new IllegalArgumentException(s"Invalid S3 bucket path was passed") // Impossible
      }

    override def toString: String = path
  }

  implicit val s3folderReads = scopt.Read.reads(S3Folder.coerce)

  object s3FolderSerializer extends CustomSerializer[S3Folder]((formats) =>
    (
      {
        case JString(s) => S3Folder.parse(s) match {
          case Right(folder) => folder
          case Left(error) => throw new MappingException(error)
        }
      },

      {
        case a: S3Folder => JString(a.path)
      }
    )
  )

  private def correctlyPrefixed(s: String): Boolean =
    supportedPrefixes.foldLeft(false) { (result, prefix) =>
      result || s.startsWith(s"$prefix://")
    }

  private[core] def fixPrefix(s: String): String =
    if (s.startsWith("s3n")) "s3" + s.stripPrefix("s3n")
    else if (s.startsWith("s3a")) "s3" + s.stripPrefix("s3a")
    else s

  private def appendTrailingSlash(s: String): S3Folder =
    if (s.endsWith("/")) new S3Folder(s)
    else new S3Folder(s + "/")
}
