package com.test_task.spark

import com.test_task.LogSupport
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import java.util.Properties
import scala.util.Try


object KafkaUtils extends LogSupport {

  case class KafkaConfig(brokerUrl: String)

  def getSingleTopicLastOffset(topic: String, kafkaConfig: KafkaConfig): Try[Long] = Try {
    val KafkaConfig(brokerUrl) = kafkaConfig
    log.info(s"Trying to get last offset from TOPIC: '$topic'")
    import scala.collection.JavaConverters._
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer(props)
    val topicPart = new TopicPartition(topic, 0)
    consumer.assign(Seq(topicPart).asJava)
    consumer.seekToEnd(Seq(topicPart).asJava)
    val offset = consumer.position(topicPart)
    log.info(s"Last offset from TOPIC: '$topic' is '$offset'")
    offset
  }

}
