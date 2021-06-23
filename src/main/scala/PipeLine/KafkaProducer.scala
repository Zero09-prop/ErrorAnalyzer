package PipeLine

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.io.Source

object KafkaProducer extends App {

  val topicName = "test"

  private def createProducer: Properties = {
    val producerProperties = new Properties()
    producerProperties.setProperty(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      "localhost:9092"
    )
    producerProperties.setProperty(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer].getName
    )
    producerProperties.setProperty(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer].getName
    )
    producerProperties
  }

  val producer = new KafkaProducer[String, String](createProducer)

  val source = Source.fromFile("server.log")

  val lines = source.getLines()

  var key = 1
  for (line <- lines) {
    Thread.sleep(2000)
    producer.send(new ProducerRecord[String, String](topicName, key.toString, line))
    key += 1
  }
  source.close()
  producer.flush()
}
