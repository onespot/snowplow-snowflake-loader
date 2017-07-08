/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader

object Main {
  def main(args: Array[String]): Unit = {
    LoaderConfig.parse(args) match {
      case Some(Right(config: LoaderConfig.LoadConfig)) =>
        println("Loading...")
        Loader.run(config)
      case Some(Right(config: LoaderConfig.SetupConfig)) =>
        println("Setting up...")
        Initializer.run(config)
      case Some(Left(error)) =>
        println(error)
        sys.exit(1)
      case None =>
        sys.exit(1)
    }
  }
}
