/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowflake.transformer.onespot

// Jackson

// Scalaz

// Scopt

// Snowplow

/**
  * Case class representing the configuration for the shred job.
  *
  * @param inFolder  Folder where the input events are located
  * @param outFolder Output folder where the shredded events will be stored
  * @param awsRegion AWS region
  */
case class OnespotTransformerJobConfig(
                                        manifestKey: String = "",
                                        inFolder: String = "",
                                        outFolder: String = "",
                                        awsRegion: String = "",
                                        manifest: String = ""
                                      )


object OnespotTransformerJobConfig {
  private val parser = new scopt.OptionParser[OnespotTransformerJobConfig]("EventsToParquetJob") {
    head("EventsToParquetJob")
    opt[String]("input-folder").required().valueName("<input folder>")
      .action((f, c) => c.copy(inFolder = f))
      .text("Folder where the input events are located")
    opt[String]("output-folder").required().valueName("<output folder>")
      .action((f, c) => c.copy(outFolder = f))
      .text("Output folder where the shredded events will be stored")
    opt[String]("aws-region").required().valueName("<aws region>")
      .action((f, c) => c.copy(awsRegion = f))
      .text("AWS region")
    opt[String]("manifest-key").required().valueName("<run id>")
      .action((f, c) => c.copy(manifestKey = f))
      .text("The manifest key for the manifest table")
    opt[String]("manifest-table").required().valueName("<manifest table name>")
      .action((f, c) => c.copy(manifest = f))
      .text("Dynamo db manifest table name")
    help("help").text("Prints this usage text")
  }

  /**
    * Load a EventsToParquetJobConfig from command line arguments.
    *
    * @param args The command line arguments
    * @return The job config or one or more error messages boxed in a Scalaz Validation Nel
    */
  def loadConfigFrom(args: Array[String]): Option[OnespotTransformerJobConfig] =
    parser.parse(args, OnespotTransformerJobConfig()) match {
      // We try to build the resolver early to detect failures before starting the job
      case Some(c) => Some(c)
      case None => throw new RuntimeException("Failed to parse cli config")
    }
}


