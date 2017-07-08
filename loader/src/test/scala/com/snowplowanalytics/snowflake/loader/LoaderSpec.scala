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

import ast.{ SnowflakeDatatype, Insert, Select }
import com.snowplowanalytics.snowflake.core.RunId

class LoaderSpec extends Specification { def is = s2"""
  Parse context column name as ARRAY type $e1
  Parse unstruct event column name as OBJECT type $e2
  Fail to parse invalid column name $e3
  Fail with exception to get list of columns $e4
  Build valid INSERT statement $e5
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
}
