package com.startdataengineering

import java.util.Properties

// Flink streaming
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}


// Spark streaming
// https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html

object FraudDetectionJob {

  val consumeTopic = "server-logs"
  val produceTopic = "alerts"

  def getProperties(): Properties = {
    val props = new Properties()
    // boostrap-server is the url kafka uses to fetch initial cluster metadata
    props.put("bootstrap.server", "localhost:9092")
    props
  }

  /**
   * Input format from consumeTopic is: (eventId,accountId,eventType,locationCountry,eventTimeStamp)
   * 
   * We define a data pipeline
   * Consume events from consumeTopic with FlinkKafkaConsumer
   * ^ Split (events record: String) with key (accountId) and do processing
   * Publish processed records to produceTopic
   */
  @throws[Exception]
  def main(args: Array[String]): Unit = {

    // Create streaming environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // Creating a consumer to read events from Apache Kafka
    // Deserializes from string format of producer
    val props = getProperties()
    val consumer = new FlinkKafkaConsumer[String](consumeTopic, new SimpleStringSchema(), props)
    consumer.setStartFromEarliest() // Consume from first event
    // Detecting fraud and generating alert events
    val events = env
      .addSource(consumer)
      .name("incoming-events")
    // 
    // Do processing on incoming events to see if alert is generated
    val alerts: DataStream[String] = events
      .keyBy(event => event.split(",")(1))  // Define keyed stream on accountId
      .process(new FraudDetection)  // Apply given KeyedProcessFunction FraudDetection on stream
      .name("fraud-detector")
    // publish processed data to new kafka topic "alerts"
    val alertsProducer = new FlinkKafkaProducer[String](produceTopic, new SimpleStringSchema(), props)
    alerts.addSink(alertsProducer).name("send-alerts")
    // Writing server logs to a PostgreSQL DB (hint: Sink)
    events.addSink(new ServerLogSink).name("event-log")

    // execute the job
    env.execute("Fraud Detection")
  }

}