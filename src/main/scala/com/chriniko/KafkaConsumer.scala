package com.chriniko

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.Future
import scala.util.{Failure, Success}

object KafkaConsumer {

  def main(args: Array[String]): Unit = {

    val topicName = "some-strings"

    implicit val system: ActorSystem = ActorSystem("QuickStart")

    implicit val dispatcher = system.dispatcher

    val config = system.settings.config.getConfig("akka.kafka.consumer")

    val consumerSettings =
      ConsumerSettings(config, new StringDeserializer, new ByteArrayDeserializer)
        .withBootstrapServers("localhost:9092")
        .withGroupId("group1") // Note: change group id to play with consumer settings...
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        //.withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        //.withProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")

        .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")


    def business(key: String, value: Array[Byte]): Future[Done] = {
      Future {
        println(s"consumed message: ${value.map(_.toChar).mkString}")
        Done.done()
      }
    }

    val committerSettings = CommitterSettings(system)

    val control: DrainingControl[Done] =
      Consumer
        .committableSource(consumerSettings, Subscriptions.topics(topicName))
        .mapAsync(1) { msg =>
          business(msg.record.key, msg.record.value).map(_ => msg.committableOffset)
        }
        .toMat(Committer.sink(committerSettings))(DrainingControl.apply)
        .run()

    val streamComplete: Future[Done] = control.drainAndShutdown()

    streamComplete onComplete {
      case Success(_) => println("Producer Done"); system.terminate()
      case Failure(err) => println(s"Producer Error: ${err.toString}"); system.terminate()
    }


  }


}
