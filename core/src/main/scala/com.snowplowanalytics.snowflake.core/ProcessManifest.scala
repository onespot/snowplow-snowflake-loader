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
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import org.joda.time.{DateTime, DateTimeZone}

import com.snowplowanalytics.snowplow.analytics.scalasdk.RunManifests
import com.snowplowanalytics.snowflake.generated.ProjectMetadata

/**
  * Entity responsible for getting all information about (un)processed folders
  */
trait ProcessManifest extends Product with Serializable {
  // Loader-specific functions
  def markLoaded(tableName: String, runId: String): Unit
  def scan(tableName: String): Either[String, List[RunId]]

  // Transformer-specific functions
  def add(tableName: String, runId: String): Unit
  def markProcessed(tableName: String, runId: String, shredTypes: List[String], outputPath: String): Unit
  def getUnprocessed(manifestTable: String, enrichedInput: String): Either[String, List[String]]
}

/**
 * Helper module for working with process manifest
 */
object ProcessManifest {

  type DbItem = JMap[String, AttributeValue]

  /** Get DynamoDB client */
  def getDynamoDb(awsAccessKey: String, awsSecretKey: String, awsRegion: String) = {
    val credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    val provider = new AWSStaticCredentialsProvider(credentials)

    AmazonDynamoDBClientBuilder.standard().withRegion(awsRegion).withCredentials(provider).build()
  }

  /** Get S3 client */
  def getS3(awsAccessKey: String, awsSecretKey: String, awsRegion: String) = {
    val credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    val provider = new AWSStaticCredentialsProvider(credentials)

    AmazonS3ClientBuilder.standard().withRegion(awsRegion).withCredentials(provider).build()
  }

  trait Loader {
    def markLoaded(tableName: String, runId: String): Unit
    def scan(tableName: String): Either[String, List[RunId]]
  }

  /** Entity being able to return processed folders from real-world DynamoDB table */
  trait AwsScan {

    def dynamodbClient: AmazonDynamoDB

    /** Get all folders with their state */
    def scan(tableName: String): Either[String, List[RunId]] = {

      def getRequest = new ScanRequest().withTableName(tableName)

      @tailrec def go(last: ScanResult, acc: List[DbItem]): List[DbItem] = {
        Option(last.getLastEvaluatedKey) match {
          case Some(key) =>
            val req = getRequest.withExclusiveStartKey(key)
            val response = dynamodbClient.scan(req)
            val items = response.getItems
            go(response, items.asScala.toList ++ acc)
          case None => acc
        }
      }

      val scanResult = try {
        val firstResponse = dynamodbClient.scan(getRequest)
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

  /** Entity being able to mark folder as processed in real-world DynamoDB table */
  trait AwsLoader { Loader =>

    def dynamodbClient: AmazonDynamoDB

    def markLoaded(tableName: String, runId: String): Unit = {
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

      val _ = dynamodbClient.updateItem(request)
    }
  }

  case class AwsProcessingManifest(s3Client: AmazonS3, dynamodbClient: AmazonDynamoDB)
    extends ProcessManifest
      with Loader
      with AwsLoader
      with AwsScan {

    def add(tableName: String, runId: String): Unit = {
      val now = (DateTime.now(DateTimeZone.UTC).getMillis / 1000).toInt

      val request = new PutItemRequest()
        .withTableName(tableName)
        .withItem(Map(
          RunManifests.DynamoDbRunIdAttribute -> new AttributeValue(runId),
          "AddedAt" -> new AttributeValue().withN(now.toString),
          "AddedBy" -> new AttributeValue(ProjectMetadata.version),
          "ToSkip" -> new AttributeValue().withBOOL(false)
        ).asJava)

      val _ = dynamodbClient.putItem(request)
    }

    def markProcessed(tableName: String, runId: String, shredTypes: List[String], outputPath: String): Unit = {
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
          "SavedTo" -> new AttributeValueUpdate().withValue(new AttributeValue(Config.fixPrefix(outputPath)))
        ).asJava)

      val _ = dynamodbClient.updateItem(request)
    }

    /** Check if set of run ids contains particular folder */
    def contains(state: List[RunId], folder: String): Boolean =
      state.map(folder => folder.runId).contains(folder)

    def getUnprocessed(manifestTable: String, enrichedInput: String): Either[String, List[String]] = {
      val allRuns = RunManifests.listRunIds(s3Client, enrichedInput)

      scan(manifestTable) match {
        case Right(state) => Right(allRuns.filterNot(run => contains(state, run)))
        case Left(error) => Left(error)
      }
    }
  }

  case class DryRunProcessingManifest(dynamodbClient: AmazonDynamoDB) extends Loader with AwsScan {
    def markLoaded(tableName: String, runId: String): Unit =
      println(s"Marking runid [$runId] processed (dry run)")
  }

  case class AwsLoaderProcessingManifest(dynamodbClient: AmazonDynamoDB) extends Loader with AwsLoader with AwsScan
}
