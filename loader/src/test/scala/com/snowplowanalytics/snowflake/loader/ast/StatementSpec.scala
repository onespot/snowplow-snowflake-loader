/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader.ast

import org.specs2.Specification

class StatementSpec extends Specification { def is = s2"""
  Transform CREATE TABLE AST into String $e1
  Transform COPY INTO AST into String $e2
  Transform INSERT INTO AST into String $e3
  Transform SHOW into String $e4
  """

  def e1 = {
    val columns = List(
      Column("id", SnowflakeDatatype.Number(2, 6), notNull = true),
      Column("foo", SnowflakeDatatype.Varchar(128), unique = true),
      Column("long_column_name", SnowflakeDatatype.DoublePrecision, unique = true, notNull = true),
      Column("baz", SnowflakeDatatype.Variant))
    val input = CreateTable("nonatomic", "data", columns, None)

    val result = input.getStatement.value
    val expected =
      """|CREATE TABLE IF NOT EXISTS nonatomic.data (
         |  id               NUMBER(2,6) NOT NULL,
         |  foo              VARCHAR(128) UNIQUE,
         |  long_column_name DOUBLE PRECISION NOT NULL UNIQUE,
         |  baz              VARIANT
         |)""".stripMargin

    result must beEqualTo(expected)
  }

  def e2 = {
    val columns = List("id", "foo", "fp_id", "json")
    val input = CopyInto(
      "some_schema",
      "some_table",
      columns,
      CopyInto.From("other_schema", "stage_name", "path/to/dir"),
      CopyInto.AwsCreds("AAA", "xyz"),
      CopyInto.FileFormat("third_schema", "format_name"))

    val result = input.getStatement.value
    val expected = "COPY INTO some_schema.some_table(id,foo,fp_id,json) " +
      "FROM @other_schema.stage_name/path/to/dir " +
      "CREDENTIALS = (AWS_KEY_ID = 'AAA' AWS_SECRET_KEY = 'xyz') " +
      "FILE_FORMAT = (FORMAT_NAME = 'third_schema.format_name')"

    result must beEqualTo(expected)
  }

  def e3 = {
    val columns = List(
      Select.CastedColumn("orig_col", "dest_column", SnowflakeDatatype.Variant),
      Select.CastedColumn("orig_col", "next", SnowflakeDatatype.DoublePrecision),
      Select.CastedColumn("orig_col", "third", SnowflakeDatatype.Number(1, 2)))
    val select = Select(columns, "some_schema", "tmp_table")
    val input = Insert.InsertQuery("not_atomic", "events", List("one", "two", "three"), select)

    val result = input.getStatement.value
    val expected = "INSERT INTO not_atomic.events(one,two,three) " +
      "SELECT orig_col:dest_column::VARIANT, orig_col:next::DOUBLE PRECISION, orig_col:third::NUMBER(1,2) " +
      "FROM some_schema.tmp_table"

    result must beEqualTo(expected)
  }

  def e4 = {
    val ast = Show.ShowStages(Some("s3://archive"), Some("atomic"))
    val result = ast.getStatement.value
    val expected = "SHOW stages LIKE 's3://archive' IN atomic"
    result must beEqualTo(expected)
  }
}
