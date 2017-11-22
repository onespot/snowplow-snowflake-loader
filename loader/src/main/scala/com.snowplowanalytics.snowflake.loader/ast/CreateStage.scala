/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader.ast

import com.snowplowanalytics.snowflake.core.Config.S3Folder

case class CreateStage(name: String, url: S3Folder, fileFormat: String, schema: String)
