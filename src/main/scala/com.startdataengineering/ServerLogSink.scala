package com.startdataengineering

// Writing server logs to PostgreSQL DBpackage com.startdataengineering

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.TimeZone

import java.util.Properties
import java.io.FileInputStream
import java.io.File
import com.startdataengineering.model.ServerLog

/**
 * 
 * 
    open a PostgreSQL DB JDBC connection in the open method and create a prepared statement which is used to insert data into the database.
    In the invoke method create a batch of statements(each of which are insert statements). Execute the batch after you have 10,000 insert statements. This is done to reduce making a database trip for each event.
    In the close method, close the opened connection from step 1.

 */
class ServerLogSink extends RichSinkFunction[String] {
  private val INSERT_CASE = "INSERT INTO server_log " +
    "(eventId, userId, eventType, locationCountry, eventTimeStamp) " +
    "VALUES (?, ?, ?, ?, ?)"

  private val COUNTRY_MAP = Map(
    "USA" -> "United States of America",
    "IN" -> "India", "UK" -> "United Kingdom", "CA" -> "Canada",
    "AU" -> "Australia", "DE" -> "Germany", "ES" -> "Spain",
    "FR" -> "France", "NL" -> "New Zealand", "SG" -> "Singapore",
    "RU" -> "Russia", "JP" -> "Japan", "BR" -> "Brazil", "CN" -> "China",
    "O" -> "Other")    

  private val dtFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
  dtFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

  // Postgres Connection and SQL statement to send
  private var conn: Connection = _ 
  private var statement: PreparedStatement = _
  private var batchCount: Int = 0
  private val MAX_BATCH_SIZE: Int = 10000

  /**
   * Get properties for Postgres Driver.
   * Contains URL, username and password for DriverManager.Connection
   */
  def getProperties(): Properties = {
    val props = new Properties()
    props.load(new FileInputStream(new File("ServerLogSink.properties")));
    props
  }

  /** 
   * Initialization function for ServerLogSink
   */
  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    // Open connection with Postgres DB
    val props = getProperties()
    val url = props.getProperty("url")
    val username = props.getProperty("username")
    val password = props.getProperty("password")
    conn = DriverManager.getConnection(s"${url}?user=${username}&password=${password}")
    // conn = DriverManager.getConnection("jdbc:postgresql://postgres:5432/events?user=startdataengineer&password=password")
    statement = conn.prepareStatement(INSERT_CASE)
  }

  /** 
   * Function evoked for each server-log event in the data stream.
   * Adds INSERT statement to batch of INSERTs
   * Executes INSERT to postgres server when 10000 inserts have been accumulated.
   */
  @throws[Exception]
  override def invoke(entity: String, ctx: SinkFunction.Context): Unit =    {
    // Create serverLog from String and substitute values into INSERT statement
    val sl = ServerLog.fromString(entity)

    statement.setString(1, sl.eventId)
    statement.setInt(2, sl.accountId)
    statement.setString(3, sl.eventType)
    statement.setString(4, COUNTRY_MAP.getOrElse(sl.locationCountry, "Other"))
    statement.setString(5, dtFormat.format(sl.eventTimeStamp * 1000L))
    statement.addBatch()
    batchCount = batchCount + 1
    // write to DB once we have MAX_BATCH_SIZE events accumulated
    if(batchCount >= MAX_BATCH_SIZE) {
      statement.executeBatch()
      batchCount = 0
    }
  
  }

  @throws[Exception]
  override def close(): Unit =   {
    // Connection.close
    conn.close()
  }

}