/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader.ast

import org.specs2.Specification

import scala.io.Source

class AtomicDefSpec  extends Specification { def is = s2"""
  CREATE atomic.events $e1
  """

  import AtomicDefSpec._

  def e1 = {
    val referenceStream = getClass.getResourceAsStream("/sql/atomic-def.sql")
    val expectedLines = Source.fromInputStream(referenceStream).getLines().toList
    val expected = normalizeSql(expectedLines)

    val resultLines = AtomicDef.getTable().getStatement.value.split("\n").toList
    val result = normalizeSql(resultLines)

    result must beEqualTo(expected)
  }
}

object AtomicDefSpec {
  /** Remove comments and formatting */
  def normalizeSql(lines: List[String]) = lines
    .map(_.dropWhile(_.isSpaceChar))
    .map(line => if (line.startsWith("--")) "" else line)
    .filterNot(_.isEmpty)
    .map(_.replaceAll("""\s+""", " "))
    .map(_.replaceAll(""",\s""", ","))

}
