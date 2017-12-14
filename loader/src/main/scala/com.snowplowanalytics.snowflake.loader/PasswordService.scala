/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowflake.loader

import scala.util.control.NonFatal

import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest

object PasswordService {

  /**
    * Get value from AWS EC2 Parameter Store
    * @param name systems manager parameter's name with SSH key
    * @return decrypted string with key
    */
  def getKey(name: String): Either[String, String] = {
    try {
      val client = AWSSimpleSystemsManagementClientBuilder.defaultClient()
      val req: GetParameterRequest = new GetParameterRequest().withName(name).withWithDecryption(true)
      val par = client.getParameter(req)
      Right(par.getParameter.getValue)
    } catch {
      case NonFatal(e) => Left(e.getMessage)
    }
  }
}
