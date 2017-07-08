/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */

import sbt._

object Dependencies {

  object V {
    // Java
    val hadoop           = "2.7.3"
    val snowflakeJdbc    = "3.1.1"
    // Scala
    val spark            = "2.1.0"
    val scopt            = "3.5.0"
    val analyticsSdk     = "0.2.0"
    val scalaz7          = "7.0.9"
    val json4sJackson    = "3.2.11"
    val cats             = "0.9.0"
    // Scala (test only)
    val specs2           = "2.3.13"
    val scalazSpecs2     = "0.2"
    val scalaCheck       = "1.12.2"
  }

  // Java
  val hadoop           = "org.apache.hadoop"     % "hadoop-aws"                    % V.hadoop         % "provided"
  val snowflakeJdbc    = "net.snowflake"         % "snowflake-jdbc"                % V.snowflakeJdbc

  // Scala
  val spark            = "org.apache.spark"      %% "spark-core"                   % V.spark          % "provided"
  val scopt            = "com.github.scopt"      %% "scopt"                        % V.scopt
  val analyticsSdk     = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
  val scalaz7          = "org.scalaz"            %% "scalaz-core"                  % V.scalaz7
  val json4sJackson    = "org.json4s"            %% "json4s-jackson"               % V.json4sJackson
  val cats             = "org.typelevel"         %% "cats"                         % V.cats

  // Scala (test only)
  val specs2           = "org.specs2"            %% "specs2"                       % V.specs2         % "test"
  val scalazSpecs2     = "org.typelevel"         %% "scalaz-specs2"                % V.scalazSpecs2   % "test"
  val scalaCheck       = "org.scalacheck"        %% "scalacheck"                   % V.scalaCheck     % "test"
}
