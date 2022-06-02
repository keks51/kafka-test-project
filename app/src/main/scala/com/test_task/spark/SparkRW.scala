package com.test_task.spark

import com.test_task.Domain.Order
import com.test_task.LogSupport
import com.test_task.spark.KafkaUtils.KafkaConfig
import com.test_task.spark.PostgresConn.PostgresConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.StringType

import scala.util.Try


object SparkRW extends LogSupport {

  def withSparkSession(appName: String, hdfsHost: String)(f: SparkSession => Unit): Try[Unit] = Try {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()
    spark.conf.set("fs.defaultFS", hdfsHost)
    f(spark)
    spark.close()
  }

  def readUserDS(config: PostgresConfig)(implicit spark: SparkSession): Try[DataFrame] = Try {
    val PostgresConfig(host, port, user, pass, db, _) = config
    val df = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://$host:$port/$db")
      .option("dbtable", "users")
      .option("user", user)
      .option("password", pass)
      .option("driver", "org.postgresql.Driver")
      .load()
    df
  }

  def readTopicBatch(topic: String,
                     startOffset: Long,
                     endOffset: Long,
                     kafkaConfig: KafkaConfig)(implicit spark: SparkSession): Try[DataFrame] = Try {
    val KafkaConfig(brokerUrl) = kafkaConfig
    log.info(s"Loading records from topic: $topic using spark with offset range ($startOffset - $endOffset)")
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerUrl)
      .option("subscribe", topic)
      .option("startingOffsets", s"""{"$topic":{"0":$startOffset}}""")
      .option("endingOffsets", s"""{"$topic":{"0":$endOffset}}""")
      .option("includeHeaders", "true")
      .load()
      .select(from_json(col("value").cast(StringType), Encoders.product[Order].schema).as("json"))
      .select("json.*")
  }

  def writeTopicBatch(df: DataFrame, savePath: String): Try[Unit] = Try {
    log.info(s"Saving data in path: '$savePath'")
    df.write.mode(SaveMode.Append).parquet(savePath)
    log.info(s"Successfully saved data in path: '$savePath'")
  }

}
