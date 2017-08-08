/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader

import ast._

import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

object Database {
  @throws[SQLException]
  def getConnection(config: LoaderConfig): Connection = {
    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")

    // Build connection properties
    val properties = new Properties()
    properties.put("user", config.snowflakeUser)
    properties.put("password", config.snowflakePassword)
    properties.put("account", config.snowflakeAccount)
    properties.put("warehouse", config.snowflakeWarehouse)
    properties.put("db", config.snowflakeDb)
    properties.put("schema", config.snowflakeSchema)

    val connectStr = s"jdbc:snowflake://${config.snowflakeAccount}.snowflakecomputing.com"
    DriverManager.getConnection(connectStr, properties)
  }

  /** Execute SQL statement */
  def execute[S: Statement](connection: Connection, ast: S): Unit = {
    val jdbcStatement = connection.createStatement()
    jdbcStatement.execute(ast.getStatement.value)
    jdbcStatement.close()
  }

  /** Execute SQL statement and print status */
  def executeAndOutput[S: Statement](connection: Connection, ast: S): Unit = {
    val statement = connection.createStatement()
    val rs = statement.executeQuery(ast.getStatement.value)
    while (rs.next()) {
      println(rs.getString("status"))
    }
    statement.close()
  }
}
