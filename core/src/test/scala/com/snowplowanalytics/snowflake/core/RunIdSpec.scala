package com.snowplowanalytics.snowflake.core

import collection.mutable

import org.specs2.Specification

import com.amazonaws.services.dynamodbv2.model.AttributeValue

import org.joda.time.{ DateTime, DateTimeZone }

import com.snowplowanalytics.snowflake.core.RunId._
import com.snowplowanalytics.snowflake.core.Config.S3Folder.{coerce => s3}

class RunIdSpec extends Specification { def is = s2"""
  Parse valid FreshRunId $e2
  Parse valid ProcessedRunId $e3
  Parse valid LoadedRunId $e4
  Parse valid RunId with unknown field $e5
  Fail to parse RunId without AddedAt $e1
  Fail to parse RunId with with ProcessedAt, but without ShredTypes $e6
  Fail to parse RunId with with invalid type $e7
  """

  def e2 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "AddedAt" -> new AttributeValue().withN("1502357136"),
      "AddedBy" ->  new AttributeValue("some-transformer"),
      "ToSkip" ->  new AttributeValue().withBOOL(false)
    )
    val result = RunId.parse(input)
    result must beRight(FreshRunId("enriched/archived/", new DateTime(1502357136000L).withZone(DateTimeZone.UTC), "some-transformer", false))
  }

  def e3 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "AddedAt" -> new AttributeValue().withN("1502357136"),
      "ProcessedAt" -> new AttributeValue().withN("1502368136"),
      "ShredTypes" -> new AttributeValue().withL(
        new AttributeValue("unstruct_event_com_acme_event_1"),
        new AttributeValue("contexts_com_acme_context_1")
      ),
      "SavedTo" -> new AttributeValue("s3://bucket/output/archived/run-01/"),
      "AddedBy" ->  new AttributeValue("some-transformer"),
      "ToSkip" -> new AttributeValue().withBOOL(false)
    )

    val result = RunId.parse(input)
    result must beRight(ProcessedRunId(
      "enriched/archived/",
      new DateTime(1502357136000L).withZone(DateTimeZone.UTC),
      new DateTime(1502368136000L).withZone(DateTimeZone.UTC),
      List("unstruct_event_com_acme_event_1", "contexts_com_acme_context_1"),
      s3("s3://bucket/output/archived/run-01/"),
      "some-transformer",
      false)
    )
  }

  def e4 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "AddedAt" -> new AttributeValue().withN("1502357136"),
      "ProcessedAt" -> new AttributeValue().withN("1502368136"),
      "LoadedAt" -> new AttributeValue().withN("1502398136"),
      "ShredTypes" -> new AttributeValue().withL(
        new AttributeValue("unstruct_event_com_acme_event_1"),
        new AttributeValue("contexts_com_acme_context_1")
      ),
      "SavedTo" -> new AttributeValue("s3://bucket/output/archived/run-01"),
      "AddedBy" ->  new AttributeValue("some-transformer"),
      "LoadedBy" ->  new AttributeValue("loader"),
      "ToSkip" -> new AttributeValue().withBOOL(false)
    )

    val result = RunId.parse(input)
    result must beRight(LoadedRunId(
      "enriched/archived/",
      new DateTime(1502357136000L).withZone(DateTimeZone.UTC),
      new DateTime(1502368136000L).withZone(DateTimeZone.UTC),
      List("unstruct_event_com_acme_event_1", "contexts_com_acme_context_1"),
      s3("s3://bucket/output/archived/run-01/"),
      new DateTime(1502398136000L).withZone(DateTimeZone.UTC),
      "some-transformer",
      "loader"))
  }

  def e5 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "AddedAt" -> new AttributeValue().withN("1502357136"),
      "AddedBy" ->  new AttributeValue("some-transformer"),
      "UnknownAttribute" -> new AttributeValue("something required by next version"),
      "ToSkip" ->  new AttributeValue().withBOOL(false)
    )
    val result = RunId.parse(input)
    result must beRight(FreshRunId("enriched/archived/", new DateTime(1502357136000L).withZone(DateTimeZone.UTC), "some-transformer", false))
  }

  def e1 = {
    val input = mutable.Map("RunId" -> new AttributeValue("enriched/archived/"))
    val result = RunId.parse(input)
    result must beLeft("Cannot extract RunId from DynamoDB record " +
      "[Map(RunId -> {S: enriched/archived/,})]. " +
      "Errors: Required AddedAt attribute is absent, Required AddedBy attribute is absent, Required ToSkip attribute is absent")
  }


  def e6 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "AddedAt" -> new AttributeValue().withN("1502357136"),
      "ProcessedAt" -> new AttributeValue().withN("1502368136"),
      "AddedBy" ->  new AttributeValue("Transformer")
    )
    val result = RunId.parse(input)
    result must beLeft(
      "Cannot extract RunId from DynamoDB record " +
        "[Map(AddedBy -> {S: Transformer,}, AddedAt -> {N: 1502357136,}, RunId -> {S: enriched/archived/,}, ProcessedAt -> {N: 1502368136,})]. " +
        "Errors: Required ToSkip attribute is absent"
    )
  }

  def e7 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "AddedAt" -> new AttributeValue().withN("1502357136"),
      "ProcessedAt" -> new AttributeValue().withS("1502368136"),
      "ShredTypes" -> new AttributeValue().withL(
        new AttributeValue("unstruct_event_com_acme_event_1"),
        new AttributeValue("contexts_com_acme_context_1")
      ),
      "AddedBy" -> new AttributeValue().withS("backfill-script"),
      "ToSkip" ->  new AttributeValue().withBOOL(false)
    )
    val result = RunId.parse(input)
    val expectedMessage = "Cannot extract RunId from DynamoDB record " +
      "[Map(ShredTypes -> {L: [{S: unstruct_event_com_acme_event_1,}, {S: contexts_com_acme_context_1,}],}, " +
      "ToSkip -> {BOOL: false}, AddedBy -> {S: backfill-script,}, AddedAt -> {N: 1502357136,}, " +
      "RunId -> {S: enriched/archived/,}, ProcessedAt -> {S: 1502368136,})]. " +
      "Errors: Required ProcessedAt attribute has non-number type"
    result must beLeft(expectedMessage)
  }
}
