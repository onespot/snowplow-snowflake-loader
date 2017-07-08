/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.core

import cats.implicits._

import scala.collection.convert.decorateAsJava._
import scala.collection.convert.decorateAsScala._

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.s3.AmazonS3ClientBuilder

import org.joda.time.{DateTime, DateTimeZone}

import com.snowplowanalytics.snowplow.analytics.scalasdk.RunManifests

/**
 * Helper module for working with process manifest
 */
object ProcessManifest {

  /** List S3 folders not added to manifest */
  def getUnprocessed(awsAccessKey: String, awsSecretKey: String, manifestTable: String, enrichedInput: String) = {
    val credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    val provider = new AWSStaticCredentialsProvider(credentials)

    val s3Client = AmazonS3ClientBuilder.standard().withRegion("us-east-1").withCredentials(provider).build()
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
        "StartedAt" -> new AttributeValue().withN(now.toString)
      ).asJava)

    dynamoDb.putItem(request)
  }

  /** Mark runId as processed by adding `ProcessedAt` and `ShredTypes` attributes */
  def markProcessed(dynamoDb: AmazonDynamoDB, tableName: String, runId: String, shredTypes: List[String]) = {
    val now = (DateTime.now(DateTimeZone.UTC).getMillis / 1000).toInt
    val shredTypesDynamo = shredTypes.map(t => new AttributeValue(t)).asJava

    val request = new UpdateItemRequest()
      .withTableName(tableName)
      .withKey(Map(
        RunManifests.DynamoDbRunIdAttribute -> new AttributeValue(runId)
      ).asJava)
      .withAttributeUpdates(Map(
        "ProcessedAt" -> new AttributeValueUpdate().withValue(new AttributeValue().withN(now.toString)),
        "ShredTypes" ->  new AttributeValueUpdate().withValue(new AttributeValue().withL(shredTypesDynamo))
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
        "LoadedAt" -> new AttributeValueUpdate().withValue(new AttributeValue().withN(now.toString))
      ).asJava)

    dynamoDb.updateItem(request)
  }

  /** Get all folders with their state */
  def scan(dynamoDB: AmazonDynamoDB, tableName: String): Either[String, List[RunId]] = {
    val request = new ScanRequest()
      .withTableName(tableName)   // TODO: add pagination

    val result = dynamoDB.scan(request)
    val rawItems = result.getItems.asScala.map(_.asScala)
    rawItems.map(RunId.parse).toList.sequence
  }
}
