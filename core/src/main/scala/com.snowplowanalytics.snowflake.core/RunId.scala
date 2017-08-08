/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.core

import scala.collection.convert.decorateAsScala._

import cats.data.{Validated, ValidatedNel}
import cats.implicits._

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.snowplowanalytics.snowflake.core.Config.S3Folder

import org.joda.time.{ DateTime, DateTimeZone }

/**
  * S3 folder saved in processing manifest. Added by Transformer job
  * Folders (run ids) refer to original archived folders in `enriched.archive`,
  * not folders produced by transformer
  */
sealed trait RunId extends Product with Serializable {
  /** S3 path to folder in **enriched archive** (without bucket name) */
  def runId: String
  /** Time when run id started to processing by Transformer job */
  def startedAt: DateTime
}

object RunId {

  /** Raw DynamoDB record */
  type RawItem = collection.Map[String, AttributeValue]

  /** Folder that just started to processing */
  case class FreshRunId(runId: String, startedAt: DateTime) extends RunId
  /** Folder that has been processed, but not yet loaded */
  case class ProcessedRunId(runId: String, startedAt: DateTime, processedAt: DateTime, shredTypes: List[String], savedTo: S3Folder) extends RunId
  /** Folder that has been processed and loaded */
  case class LoadedRunId(runId: String, startedAt: DateTime, processedAt: DateTime, shredTypes: List[String], savedTo: S3Folder, loadedAt: DateTime) extends RunId

  /**
    * Extract `RunId` from data returned from DynamoDB
    * @param rawItem DynamoDB item from processing manifest table
    * @return either error or one of possible run ids
    */
  def parse(rawItem: RawItem): Either[String, RunId] = {
    val runId = getRunId(rawItem)
    val startedAt = getStartedAt(rawItem)
    val processedAt = getProcessedAt(rawItem)
    val savedTo = getSavedTo(rawItem)
    val shredTypes = getShredTypes(rawItem)
    val loadedAt = getLoadedAt(rawItem)

    val result = (runId |@| startedAt |@| processedAt |@| shredTypes |@| savedTo |@| loadedAt).map {
      case (run, started, None, None, None, None) =>
        FreshRunId(run, started).asRight
      case (run, started, Some(processed), Some(shredded), Some(saved), None) =>
        ProcessedRunId(run, started, processed, shredded, saved).asRight
      case (run, started, Some(processed), Some(shredded), Some(saved), Some(loaded)) =>
        LoadedRunId(run, started, processed, shredded, saved, loaded).asRight
      case (run, started, processed, shredded, saved, loaded) =>
        s"Invalid state: RunId -> $run, StartedAt -> $started, ProcessedAt -> $processed, ShredTypes -> $shredded, SavedTo -> $saved, LoadedAt -> $loaded".asLeft
    }

    result match {
      case Validated.Valid(Right(success)) => success.asRight
      case Validated.Valid(Left(error)) => error.asLeft
      case Validated.Invalid(errors) =>
        s"Cannot extract RunId from DynamoDB record [$rawItem]. Errors: ${errors.toList.mkString(", ")}".asLeft
    }
  }

  private def getRunId(rawItem: RawItem): ValidatedNel[String, String] = {
    val runId = rawItem.get("RunId") match {
      case Some(value) if value.getS != null => Right(value.getS)
      case Some(_) => Left(s"Required RunId attribute has non-string type")
      case None => Left(s"Required RunId attribute is absent")
    }
    runId.toValidatedNel
  }

  private def getStartedAt(rawItem: RawItem) = {
    val startedAt = rawItem.get("StartedAt") match {
      case Some(value) if value.getN != null =>
        Right(new DateTime(value.getN.toLong * 1000L).withZone(DateTimeZone.UTC))
      case Some(_) =>
        Left(s"Required StartedAt attribute has non-number type")
      case None =>
        Left(s"Required StartedAt attribute is absent")
    }
    startedAt.toValidatedNel
  }

  private def getProcessedAt(rawItem: RawItem) = {
    val processedAt = rawItem.get("ProcessedAt") match {
      case Some(value) if value.getN != null =>
        Right(Some(new DateTime(value.getN.toLong * 1000L).withZone(DateTimeZone.UTC)))
      case Some(_) =>
        Left(s"Required ProcessedAt attribute has non-number type")
      case None =>
        Right(None)
    }
    processedAt.toValidatedNel
  }

  private def getShredTypes(rawItem: RawItem) = {
    rawItem.get("ShredTypes") match {
      case Some(value) if value.getL == null =>
        s"Required ShredTypes attribute has non-list type in [$rawItem] record".invalidNel
      case Some(value) =>
        value.getL.asScala.toList.map { x =>
          val string = x.getS
          if (string != null)
            Validated.Valid(string)
          else
            Validated.Invalid("Invalid value in ShredTypes")
        }.sequence match {
          case Validated.Valid(list) => list.some.validNel
          case Validated.Invalid(errors) => errors.invalidNel[Option[List[String]]]
        }
      case None => none[List[String]].validNel
    }
  }

  private def getLoadedAt(rawItem: RawItem) = {
    val loadedAt = rawItem.get("LoadedAt") match {
      case Some(value) if value.getN != null =>
        Right(Some(new DateTime(value.getN.toLong * 1000L).withZone(DateTimeZone.UTC)))
      case Some(_) =>
        Left(s"Required LoadAt attribute has non-number type in [$rawItem] record")
      case None => Right(None)
    }
    loadedAt.toValidatedNel
  }

  private def getSavedTo(rawItem: RawItem) = {
    val savedTo = rawItem.get("SavedTo") match {
      case Some(value) if value.getS != null =>
        S3Folder.parse(value.getS).map(_.some)
      case Some(_) =>
        Left(s"Required SavedTo attribute has non-string type in [$rawItem] record")
      case None => Right(None)
    }
    savedTo.toValidatedNel
  }
}
