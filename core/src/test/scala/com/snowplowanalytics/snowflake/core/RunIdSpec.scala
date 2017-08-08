package com.snowplowanalytics.snowflake.core

import collection.mutable

import org.specs2.Specification

import com.amazonaws.services.dynamodbv2.model.AttributeValue

import org.joda.time.{ DateTime, DateTimeZone }

import com.snowplowanalytics.snowflake.core.RunId._

class RunIdSpec extends Specification { def is = s2"""
  Parse valid FreshRunId $e2
  Parse valid ProcessedRunId $e3
  Parse valid LoadedRunId $e4
  Parse valid RunId with unknown field $e5
  Fail to parse RunId without StartedId $e1
  Fail to parse RunId with with ProcessedAt, but without ShredTypes $e6
  Fail to parse RunId with with invalid type $e7
  """

  def e2 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "StartedAt" -> new AttributeValue().withN("1502357136")
    )
    val result = RunId.parse(input)
    result must beRight(FreshRunId("enriched/archived/", new DateTime(1502357136000L).withZone(DateTimeZone.UTC)))
  }

  def e3 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "StartedAt" -> new AttributeValue().withN("1502357136"),
      "ProcessedAt" -> new AttributeValue().withN("1502368136"),
      "ShredTypes" -> new AttributeValue().withL(
        new AttributeValue("unstruct_event_com_acme_event_1"),
        new AttributeValue("context_com_acme_context_1")
      ),
      "SavedTo" -> new AttributeValue("s3://bucket/output/archived/run-01/"))

    val result = RunId.parse(input)
    result must beRight(ProcessedRunId(
      "enriched/archived/",
      new DateTime(1502357136000L).withZone(DateTimeZone.UTC),
      new DateTime(1502368136000L).withZone(DateTimeZone.UTC),
      List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"),
      "s3://bucket/output/archived/run-01/"))
  }

  def e4 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "StartedAt" -> new AttributeValue().withN("1502357136"),
      "ProcessedAt" -> new AttributeValue().withN("1502368136"),
      "LoadedAt" -> new AttributeValue().withN("1502398136"),
      "ShredTypes" -> new AttributeValue().withL(
        new AttributeValue("unstruct_event_com_acme_event_1"),
        new AttributeValue("context_com_acme_context_1")
      ),
      "SavedTo" -> new AttributeValue("s3://bucket/output/archived/run-01"))

    val result = RunId.parse(input)
    result must beRight(LoadedRunId(
      "enriched/archived/",
      new DateTime(1502357136000L).withZone(DateTimeZone.UTC),
      new DateTime(1502368136000L).withZone(DateTimeZone.UTC),
      List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"),
      "s3://bucket/output/archived/run-01/",
      new DateTime(1502398136000L).withZone(DateTimeZone.UTC)))
  }

  def e5 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "StartedAt" -> new AttributeValue().withN("1502357136"),
      "UnknownAttribute" -> new AttributeValue("something required by next version")
    )
    val result = RunId.parse(input)
    result must beRight(FreshRunId("enriched/archived/", new DateTime(1502357136000L).withZone(DateTimeZone.UTC)))
  }

  def e1 = {
    val input = mutable.Map("RunId" -> new AttributeValue("enriched/archived/"))
    val result = RunId.parse(input)
    result must beLeft("Cannot extract RunId from DynamoDB record [Map(RunId -> {S: enriched/archived/,})]. Errors: Required StartedAt attribute is absent")
  }


  def e6 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "StartedAt" -> new AttributeValue().withN("1502357136"),
      "ProcessedAt" -> new AttributeValue().withN("1502368136")
    )
    val result = RunId.parse(input)
    result must beLeft(
      "Invalid state: RunId -> enriched/archived/, StartedAt -> 2017-08-10T09:25:36.000Z, ProcessedAt -> Some(2017-08-10T12:28:56.000Z), ShredTypes -> None, SavedTo -> None, LoadedAt -> None"
    )
  }

  def e7 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "StartedAt" -> new AttributeValue().withN("1502357136"),
      "ProcessedAt" -> new AttributeValue().withS("1502368136"),
      "ShredTypes" -> new AttributeValue().withL(
        new AttributeValue("unstruct_event_com_acme_event_1"),
        new AttributeValue("context_com_acme_context_1")
      )
    )
    val result = RunId.parse(input)
    val expectedMessage = "Cannot extract RunId from DynamoDB record " +
      "[Map(ShredTypes -> {L: [{S: unstruct_event_com_acme_event_1,}, {S: context_com_acme_context_1,}],}, StartedAt -> {N: 1502357136,}, RunId -> {S: enriched/archived/,}, ProcessedAt -> {S: 1502368136,})]. " +
      "Errors: Required ProcessedAt attribute has non-number type"
    result must beLeft(expectedMessage)
  }
}
