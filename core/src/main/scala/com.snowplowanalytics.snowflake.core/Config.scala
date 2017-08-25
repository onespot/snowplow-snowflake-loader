/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.core

/**
 * Common configuration used by Transformer and Loader
 */
object Config {

  /**
    * Extract `s3://path/run=YYYY-MM-dd-HH-mm-ss/atomic-events` part from
    * Set of prefixes that can be used in config.yml
    * In the end it won't affect how S3 is accessed
    */
  val supportedPrefixes = Set("s3", "s3n", "s3a")

  /** Weak newtype replacement to mark string prefixed with s3:// and ended with trailing slash */
  type S3Folder = String

  object S3Folder {
    def parse(s: String): Either[String, S3Folder] = s match {
      case _ if !correctlyPrefixed(s) => Left(s"Bucket name [$s] must start with s3:// prefix")
      case _ if s.length > 1024       => Left("Key length cannot be more than 1024 symbols")
      case _                          => Right(appendTrailingSlash(fixPrefix(s)))
    }
  }

  /** Split valid S3 folder path to bucket and path */
  def splitS3Folder(path: S3Folder): (String, String) =
    path.stripPrefix("s3://").split("/").toList match {
      case head :: Nil => (head, "/")
      case head :: tail => (head, tail.mkString("/") + "/")
      case Nil => throw new IllegalArgumentException(s"Invalid S3 bucket path was passed") // Impossible
    }

  private def correctlyPrefixed(s: String): Boolean =
    supportedPrefixes.foldLeft(false) { (result, prefix) =>
      result || s.startsWith(s"$prefix://")
    }

  private[core] def fixPrefix(s: String): String =
    if (s.startsWith("s3n")) "s3" + s.stripPrefix("s3n")
    else if (s.startsWith("s3a")) "s3" + s.stripPrefix("s3a")
    else s

  private def appendTrailingSlash(s: String): S3Folder =
    if (s.endsWith("/")) s
    else s + "/"
}
