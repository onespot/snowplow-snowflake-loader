/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader

import  com.snowplowanalytics.snowflake.core.Config

object Main {
  def main(args: Array[String]): Unit = {
    Config.parseLoaderCli(args) match {
      case Some(Right(config @ Config.CliLoaderConfiguration(Config.LoadCommand, _, _))) =>
        println("Loading...")
        Loader.run(config)
      case Some(Right(config @ Config.CliLoaderConfiguration(Config.SetupCommand, _, _))) =>
        println("Setting up...")
        Initializer.run(config.loaderConfig)
      case Some(Left(error)) =>
        println(error)
        sys.exit(1)
      case None =>
        sys.exit(1)
    }
  }
}
