package com.test_task.producer

import com.test_task.Domain.{Order, OrderId}
import com.test_task.LogSupport
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization._

import java.util.Properties
import java.util.concurrent.Future
import scala.util.Random


object KafkaProducerApp extends LogSupport {

  object Topic {
    val ORDERS_TOPIC = "orders"
  }

  import Topic._

  def run(args: Array[String]): Unit = {
    val BROKER_URL = args(1)

    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BROKER_URL)

    val producer = new KafkaProducer[OrderId, Order](props, new StringSerializer(), serde[Order].serializer())
    var i = -1
    while (true) {
      i = i + 1
      val id = i.toString
      val record = new ProducerRecord[OrderId, Order](ORDERS_TOPIC, id, Order(id, (Random.nextInt(10) + 1).toString, "a", 100.0))
      val res: Future[RecordMetadata] = producer.send(record)
      log.info(s"sent record(key=${record.key} value=${record.value}) meta(partition=${res.get.partition}, offset=${res.get.offset})")
      Thread.sleep(500 + Random.nextInt(500))
    }

  }

  def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
    val ser = new Serializer[A] {
      override def serialize(topic: String, data: A): Array[Byte] = data.asJson.noSpaces.getBytes
    }
    val des = new Deserializer[A] {
      override def deserialize(topic: String, data: Array[Byte]): A = {
        val str = new String(data)
        decode[A](str).toOption.orNull
      }
    }
    Serdes.serdeFrom(ser, des)
  }

}
