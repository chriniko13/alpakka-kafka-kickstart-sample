package com.chriniko

import java.util.Properties

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}
import collection.JavaConverters._

object KafkaProducer {

  def main(args: Array[String]): Unit = {


    // --- create topic section ---
    val topicName = "some-strings"

    val props = new Properties()
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    val adminClient = AdminClient.create(props)

    val numPartitions = 3
    val replicationFactor = 3.toShort
    val newTopic = new NewTopic(topicName, numPartitions, replicationFactor)
    val configs = Map(TopicConfig.COMPRESSION_TYPE_CONFIG -> "gzip")
    // settings some configs
    newTopic.configs(configs.asJava)

    val result = adminClient.createTopics(List(newTopic).asJavaCollection)
    println(s"topic created, result: ${result.values()}")


    // --- create producer section ---
    implicit val system: ActorSystem = ActorSystem("QuickStart")

    val config = system.settings.config.getConfig("akka.kafka.producer")

    val producerSettings = ProducerSettings(config, new StringSerializer, new StringSerializer)
      .withBootstrapServers("localhost:9092")

    val done: Future[Done] =
      Source(1 to 100000)
        .map(_.toString)
        .map(value => new ProducerRecord[String, String](topicName, value))
        .runWith(Producer.plainSink(producerSettings))

    implicit val ec: ExecutionContextExecutor = system.dispatcher
    done onComplete {
      case Success(_) => println("Producer Done"); system.terminate()
      case Failure(err) => println(s"Producer Error: ${err.toString}"); system.terminate()
    }

  }


}
