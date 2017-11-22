/*
 * PROPRIETARY AND CONFIDENTIAL
 *
 * Unauthorized copying of this project via any medium is strictly prohibited.
 *
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 */
package com.snowplowanalytics.snowflake.loader
package connection

import ast._
import java.sql.{DriverManager, SQLException, Connection => JdbcConnection}
import java.util.Properties

import com.snowplowanalytics.snowflake.core.Config

object Jdbc extends Connection[JdbcConnection] {

  @throws[SQLException]
  def getConnection(config: Config): JdbcConnection = {
    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")

    // US West is default: https://docs.snowflake.net/manuals/user-guide/jdbc-configure.html#jdbc-driver-connection-string
    val host = if (config.snowflakeRegion == "us-west-1")
      s"${config.account}.snowflakecomputing.com"
    else
      s"${config.account}.${config.snowflakeRegion}.snowflakecomputing.com"

    // Build connection properties
    val properties = new Properties()

    properties.put("user", config.username)
    properties.put("password", config.password)
    properties.put("account", config.account)
    properties.put("warehouse", config.warehouse)
    properties.put("db", config.database)
    properties.put("schema", config.schema)

    val connectStr = s"jdbc:snowflake://$host"
    DriverManager.getConnection(connectStr, properties)
  }

  /** Execute SQL statement */
  def execute[S: Statement](connection: JdbcConnection, ast: S): Unit = {
    val jdbcStatement = connection.createStatement()
    jdbcStatement.execute(ast.getStatement.value)
    jdbcStatement.close()
  }

  /** Begin transaction */
  def startTransaction(connection: JdbcConnection, name: Option[String]): Unit = {
    val jdbcStatement = connection.createStatement()
    jdbcStatement.execute(s"BEGIN TRANSACTION NAME ${name.getOrElse("")}")
    jdbcStatement.close()
  }

  /** Commit transaction */
  def commitTransaction(connection: JdbcConnection): Unit = {
    val jdbcStatement = connection.createStatement()
    jdbcStatement.execute("COMMIT")
    jdbcStatement.close()
  }

  def rollbackTransaction(connection: JdbcConnection): Unit = {
    val jdbcStatement = connection.createStatement()
    jdbcStatement.execute("ROLLBACK")
    jdbcStatement.close()
  }

  /** Execute SQL statement and print status */
  def executeAndOutput[S: Statement](connection: JdbcConnection, ast: S): Unit = {
    val statement = connection.createStatement()
    val rs = statement.executeQuery(ast.getStatement.value)
    while (rs.next()) {
      println(rs.getString("status"))
    }
    statement.close()
  }

  /** Execute SQL query and count rows */
  def executeAndCountRows[S: Statement](connection: JdbcConnection, ast: S): Int = {
    val statement = connection.createStatement()
    val rs = statement.executeQuery(ast.getStatement.value)
    var i = 0
    while (rs.next()) {
      i = i + 1
    }
    i
  }
}
