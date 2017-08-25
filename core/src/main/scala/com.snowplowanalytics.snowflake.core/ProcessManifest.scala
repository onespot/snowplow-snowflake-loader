/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.core

import cats.implicits._

import java.util.{Map => JMap}

import scala.annotation.tailrec
import scala.collection.convert.decorateAsJava._
import scala.collection.convert.decorateAsScala._
import scala.util.control.NonFatal

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.s3.AmazonS3ClientBuilder

import org.joda.time.{DateTime, DateTimeZone}

import com.snowplowanalytics.snowplow.analytics.scalasdk.RunManifests

import com.snowplowanalytics.snowflake.generated.ProjectMetadata

/**
 * Helper module for working with process manifest
 */
object ProcessManifest {

  type DbItem = JMap[String, AttributeValue]

  /** List S3 folders not added to manifest (in any way, including loaded, skipped, fresh etc) */
  def getUnprocessed(awsAccessKey: String, awsSecretKey: String, awsRegion: String, manifestTable: String, enrichedInput: String) = {
    val credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    val provider = new AWSStaticCredentialsProvider(credentials)

    val s3Client = AmazonS3ClientBuilder.standard().withRegion(awsRegion).withCredentials(provider).build()
    val dynamodbClient = getDynamoDb(awsAccessKey, awsSecretKey)

    val runManifest = RunManifests(dynamodbClient, manifestTable)
    runManifest.create()
    val allRuns = RunManifests.listRunIds(s3Client, enrichedInput)
    allRuns.filterNot(runManifest.contains)
  }

  /** Get DynamoDB client */
  def getDynamoDb(awsAccessKey: String, awsSecretKey: String) = {
    val credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    val provider = new AWSStaticCredentialsProvider(credentials)

    AmazonDynamoDBClientBuilder.standard().withCredentials(provider).build()
  }

  /** Add runId to manifest, with `StartedAt` attribute */
  def add(dynamoDb: AmazonDynamoDB, tableName: String, runId: String) = {
    val now = (DateTime.now(DateTimeZone.UTC).getMillis / 1000).toInt

    val request = new PutItemRequest()
      .withTableName(tableName)
      .withItem(Map(
        RunManifests.DynamoDbRunIdAttribute -> new AttributeValue(runId),
        "StartedAt" -> new AttributeValue().withN(now.toString),
        "AddedBy" -> new AttributeValue(ProjectMetadata.version),
        "ToSkip" -> new AttributeValue().withBOOL(false)
      ).asJava)

    dynamoDb.putItem(request)
  }

  /** Mark runId as processed by adding `ProcessedAt` and `ShredTypes` attributes */
  def markProcessed(dynamoDb: AmazonDynamoDB, tableName: String, runId: String, shredTypes: List[String], outputPath: String) = {
    val now = (DateTime.now(DateTimeZone.UTC).getMillis / 1000).toInt
    val shredTypesDynamo = shredTypes.map(t => new AttributeValue(t)).asJava

    val request = new UpdateItemRequest()
      .withTableName(tableName)
      .withKey(Map(
        RunManifests.DynamoDbRunIdAttribute -> new AttributeValue(runId)
      ).asJava)
      .withAttributeUpdates(Map(
        "ProcessedAt" -> new AttributeValueUpdate().withValue(new AttributeValue().withN(now.toString)),
        "ShredTypes" -> new AttributeValueUpdate().withValue(new AttributeValue().withL(shredTypesDynamo)),
        "SavedTo" -> new AttributeValueUpdate().withValue(new AttributeValue(outputPath))
      ).asJava)

    dynamoDb.updateItem(request)
  }

  /** Mark runId as loaded by adding `LoadedAt` attribute */
  def markLoaded(dynamoDb: AmazonDynamoDB, tableName: String, runId: String) = {
    val now = (DateTime.now(DateTimeZone.UTC).getMillis / 1000).toInt

    val request = new UpdateItemRequest()
      .withTableName(tableName)
      .withKey(Map(
        RunManifests.DynamoDbRunIdAttribute -> new AttributeValue(runId)
      ).asJava)
      .withAttributeUpdates(Map(
        "LoadedAt" -> new AttributeValueUpdate().withValue(new AttributeValue().withN(now.toString)),
        "LoadedBy" -> new AttributeValueUpdate().withValue(new AttributeValue(ProjectMetadata.version))
      ).asJava)

    dynamoDb.updateItem(request)
  }

  /** Get all folders with their state */
  def scan(dynamoDB: AmazonDynamoDB, tableName: String): Either[String, List[RunId]] = {

    def getRequest = new ScanRequest().withTableName(tableName)

    @tailrec def go(last: ScanResult, acc: List[DbItem]): List[DbItem] = {
      Option(last.getLastEvaluatedKey) match {
        case Some(key) =>
          val req = getRequest.withExclusiveStartKey(key)
          val response = dynamoDB.scan(req)
          val items = response.getItems
          go(response, items.asScala.toList ++ acc)
        case None => acc
      }
    }

    val scanResult = try {
      val firstResponse = dynamoDB.scan(getRequest)
      val initAcc = firstResponse.getItems.asScala.toList
      Right(go(firstResponse, initAcc).map(_.asScala))
    } catch {
      case NonFatal(e) => Left(e.toString)
    }

    for {
      items <- scanResult
      result <- items.map(RunId.parse).sequence
    } yield result
  }
}
