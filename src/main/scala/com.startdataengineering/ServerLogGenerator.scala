package com.startdataengineering

import java.time.Instant
import java.util.Properties
import java.util.UUID.randomUUID
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Random

import com.startdataengineering.model.ServerLog
/**
 * Data generator to populate Kafka topic
    open a kafka producer
    create 100,000 random server log events
    convert them to strings
    send them as values to the topic server-logs, with the key being its eventId which is a UUID(universally unique identifier)
    close the producer

 */
object ServerLogGenerator {
  private val nRecords = 100000
  private val topic = "server-logs"
  private val random = new Random

  private val locationCountry: Array[String] = Array(
    "USA", "IN", "UK", "CA", "AU", "DE", "ES", "FR", "NL", "SG", "RU", "JP", "BR", "CN", "O")

  private val eventType: Array[String] = Array(
    "click", "purchase", "login", "log-out", "delete-account", "create-account", "update-settings", "other")
 
  /**
   * Outputs new ServerLog object with randomized values
   */
  def getServerLog(): ServerLog = {
    val eventId = randomUUID.toString()
    val accountId = random.nextInt(10000)
    val curEventType = eventType.apply(random.nextInt(eventType.size))
    val curLocationCountry = locationCountry.apply(random.nextInt(locationCountry.size))
    val eventTimeStamp = Instant.now.getEpochSecond

    ServerLog(eventId, accountId, curEventType, curLocationCountry, eventTimeStamp)
  }

  /**
   * Get properties for Kafka-Producer
   */
  def getProperties(): Properties = {
    val props = new Properties()
    // boostrap-server is the url kafka uses to fetch initial cluster metadata
    props.put("bootstrap.server", "localhost:9092")
    // instruct how to turn the key and value objects the user provides with their ProducerRecord into bytes
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  /**
   * Create new Kafka producer using hard-coded properties
   * Produce 100000 records to "server-logs" topic
   */
  def main(args: Array[String]): Unit = {
    val props = getProperties()
    val producer = new KafkaProducer[String, String](props)
    var i = 0
    while (i < 100000)  {
      val log: ServerLog = getServerLog()
      // Key-Value pair to be sent to kafka without a defined partition
      val record = new ProducerRecord[String, String](topic, log.eventId, log.toString)
      producer.send(record)
      i += 1
    }
    producer.close()
  }

}