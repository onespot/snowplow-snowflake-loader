/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader

import org.specs2.Specification
import org.joda.time.DateTime
import ast.{Insert, Select, SnowflakeDatatype}
import com.snowplowanalytics.snowflake.loader.connection.DryRun
import com.snowplowanalytics.snowflake.core.{ProcessManifest, RunId}
import com.snowplowanalytics.snowflake.loader

class LoaderSpec extends Specification { def is = s2"""
  Parse context column name as ARRAY type $e1
  Parse unstruct event column name as OBJECT type $e2
  Fail to parse invalid column name $e3
  Fail with exception to get list of columns $e4
  Build valid INSERT statement $e5
  Look at output $e6
  """

  def e1 = {
    val columnName = "contexts_com_snowplowanalytics_snowplow_web_page_1"
    val expected = "contexts_com_snowplowanalytics_snowplow_web_page_1" -> SnowflakeDatatype.JsonArray
    val result = Loader.getShredType(columnName)

    result must beRight(expected)
  }

  def e2 = {
    val columnName = "unstruct_event_com_snowplowanalytics_snowplow_link_click_1"
    val expected = "unstruct_event_com_snowplowanalytics_snowplow_link_click_1" -> SnowflakeDatatype.JsonObject
    val result = Loader.getShredType(columnName)

    result must beRight(expected)
  }

  def e3 = {
    // starts with context_ instead of contexts_
    val columnName = "context_com_snowplowanalytics_snowplow_web_page_1"
    val result = Loader.getShredType(columnName)

    result must beLeft
  }

  def e4 = {
    val newColumns = List("contexts_com_acme_ctx_1", "unknown_type")
    Loader.getColumns(newColumns) must throwA[RuntimeException]
  }

  def e5 = {
    val config = LoaderConfig.LoadConfig(
      awsAccessKey = "accessKey",
      awsSecretKey = "secretKey",
      awsRegion = "awsRegion",
      manifestTable = "snoflake-manifest",
      snowflakeRegion = "ue-east-1",
      snowflakeStage = "snowplow-stage",
      snowflakeUser = "snowfplow-loader",
      snowflakePassword = "super-secret",
      snowflakeAccount = "snowplow-account",
      snowflakeWarehouse = "snowplow_wa",
      snowflakeDb = "database",
      snowflakeSchema = "not_an_atomic")

    val runId = RunId.ProcessedRunId(
      "archive/enriched/run=2017-10-09-17-40-30/",
      addedAt = DateTime.now(),       // Doesn't matter
      processedAt = DateTime.now(),   // Doesn't matter
      List(
        "contexts_com_snowplowanalytics_snowplow_web_page_1",
        "contexts_com_snowplowanalytics_snowplow_web_page_2",
        "unstruct_event_com_snowplowanalytics_snowplow_link_click_1"),
      "s3://acme-snowplow/snowflake/run=2017-10-09-17-40-30/",
      "some-script",
      false)

    // Some to use .like matcher
    val result = Some(Loader.getInsertStatement(config, runId))

    result must beSome.like {
      case Insert.InsertQuery(schema, table, columns, Select(sColumns, sSchema, sTable)) =>
        // INSERT
        val schemaResult = schema must beEqualTo("not_an_atomic")
        val tableResult = table must beEqualTo("events")
        val columnsAmount = columns must haveLength(131)
        val exactColumns = columns must containAllOf(List(
          "unstruct_event_com_snowplowanalytics_snowplow_link_click_1",
          "contexts_com_snowplowanalytics_snowplow_web_page_2",
          "app_id",
          "page_url"))

        // SELECT
        val sSchemaResult = sSchema must beEqualTo("not_an_atomic")
        val sTableResult = sTable must beEqualTo("snowplow_tmp_run_2017_10_09_17_40_30")
        val sColumnsAmount = sColumns must haveLength(131)
        val sExactColumns = sColumns must containAllOf(List(
          Select.CastedColumn("enriched_data","unstruct_event_com_snowplowanalytics_snowplow_link_click_1", SnowflakeDatatype.JsonObject),
          Select.CastedColumn("enriched_data", "contexts_com_snowplowanalytics_snowplow_web_page_1", SnowflakeDatatype.JsonArray),
          Select.CastedColumn("enriched_data", "true_tstamp", SnowflakeDatatype.Timestamp),
          Select.CastedColumn("enriched_data", "refr_domain_userid", SnowflakeDatatype.Varchar(36))))

        schemaResult.and(tableResult).and(columnsAmount)
          .and(sSchemaResult).and(sTableResult).and(sColumnsAmount)
          .and(exactColumns).and(sExactColumns)
    }
  }

  def e6 = {
    val connection = new DryRun()
    val config = LoaderConfig.LoadConfig("access", "secret", "us-east-1", "manifest", "eu-central-1", "s3://archive", "user", "pass", "snowplow-acc", "wh", "db", "atomic", false)
    Loader.exec(DryRun, connection, new loader.LoaderSpec.ProcessingManifestTest, config)
    val expected = List(
      "SHOW schemas LIKE 'atomic'",
      "SHOW stages LIKE 's3://archive' IN atomic",
      "SHOW tables LIKE 'events' IN atomic",
      "SHOW file formats LIKE 'snowplow_enriched_json' IN atomic",
      "SHOW warehouses LIKE 'wh'",
      "USE WAREHOUSE wh",
      "ALTER WAREHOUSE wh RESUME",
      "INFO: New transaction snowplow-enriched/good/run=2017-12-10-14-30-35 started",
      "ALTER TABLE atomic.events ADD COLUMN contexts_com_acme_something_1 ARRAY",
      "CREATE TEMPORARY TABLE IF NOT EXISTS atomic.snowplow_tmp_run_2017_12_10_14_30_35 (\n  enriched_data OBJECT NOT NULL\n)",
      // INSERT INTO
      "INFO: Transaction [snowplow-enriched/good/run=2017-12-10-14-30-35] successfully closed"
    )
    val result = connection.getResult
    result must containAllOf(expected).inOrder
  }
}

object LoaderSpec {
  class ProcessingManifestTest extends ProcessManifest.Loader {
    private val loaded = collection.mutable.ListBuffer.newBuilder[(String, String)]

    def markLoaded(tableName: String, runid: String): Unit = {
      loaded += ((tableName, runid))
    }

    override def scan(tableName: String): Either[String, List[RunId]] = Right(
      List(
        RunId.ProcessedRunId(
          "enriched/good/run=2017-12-10-14-30-35",
          DateTime.parse("2017-12-10T01:20+02:00"),
          DateTime.parse("2017-12-10T01:20+02:00"),
          List("contexts_com_acme_something_1"),
          "s3://archive/run=2017-12-10-14-30-35/", "0.2.0", false))
    )
  }
}
