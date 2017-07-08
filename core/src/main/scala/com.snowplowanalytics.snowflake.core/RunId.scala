/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.core

import scala.collection.convert.decorateAsScala._

import cats.implicits._

import com.amazonaws.services.dynamodbv2.model.AttributeValue

import org.joda.time.DateTime

/**
  * S3 folder saved in processing manifest. Added by Transformer job
  */
sealed trait RunId {
  /** S3 path to folder (without bucket name) */
  def runId: String
  /** Time when run id started to processing by Transformer job */
  def startedAt: DateTime
}

/** Folder that just started to processing */
case class FreshRunId(runId: String, startedAt: DateTime) extends RunId
/** Folder that has been processed, but not yet loaded */
case class ProcessedRunId(runId: String, startedAt: DateTime, processedAt: DateTime, shredTypes: List[String]) extends RunId
/** Folder that has been processed and loaded */
case class LoadedRunId(runId: String, startedAt: DateTime, processedAt: DateTime, shredTypes: List[String], loadedAt: DateTime) extends RunId

object RunId {

  /**
    * Extract `RunId` from data returned from DynamoDB
    * @param rawItem DynamoDB item from processing manifest table
    * @return either error or one of possible run ids
    */
  def parse(rawItem: collection.Map[String, AttributeValue]): Either[String, RunId] = {
    val runId = rawItem.get("RunId") match {
      case Some(value) => Right(value.getS)   // TODO: check null
      case None => Left(s"Required RunId attribute is absent in [$rawItem] record")
    }

    val startedAt = rawItem.get("StartedAt") match {
      case Some(value) => Right(new DateTime(value.getN.toLong * 1000L))   // TODO: check null
      case None => Left(s"Required StartedAt attribute is absent in [$rawItem] record")
    }

    val processedAt = rawItem.get("StartedAt") match {
      case Some(value) => Right(Some(new DateTime(value.getN.toLong * 1000L)))   // TODO: check null
      case None => Right(None)
    }

    val shredTypes: Either[String, Option[List[String]]] =
      rawItem.get("ShredTypes") match {
        case Some(value) =>
          val attributeValues: Option[Either[String, List[String]]] =
            Option(value.getL).map(_.asScala.toList.map { x =>
              val string = x.getS
              if (string != null) Right(string) else Left("Invalid value in ShredTypes")
            }.sequence)
          attributeValues.sequence
        case None => Right(None)
      }

    val loadedAt = rawItem.get("LoadedAt") match {
      case Some(value) =>
        val loaded = Option(value.getN).map(l => new DateTime(l.toLong * 1000L))
        Right(loaded)   // TODO: check null
      case None => Right(None)
    }

    (runId, startedAt, processedAt, shredTypes, loadedAt) match {
      case (Right(run), Right(started), Right(None), Right(None), Right(None)) =>
        Right(FreshRunId(run, started))

      case (Right(run), Right(started), Right(Some(processed)), Right(Some(shredded)), Right(None)) =>
        Right(ProcessedRunId(run, started, processed, shredded))
      case (Right(run), Right(started), Right(Some(processed)), Right(Some(shredded)), Right(Some(loaded))) =>
        Right(LoadedRunId(run, started, processed, shredded, loaded))
      case _ =>
        Left(s"Invalid process manifest state: [$rawItem]")
    }
  }
}
