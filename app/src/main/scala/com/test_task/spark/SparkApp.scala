package com.test_task.spark

import com.test_task.LogSupport
import com.test_task.spark.KafkaUtils.KafkaConfig
import com.test_task.spark.PostgresConn.PostgresConfig
import com.test_task.spark.Utils.getTimestampNow
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Connection
import scala.util.{Failure, Success, Try}


object SparkApp extends LogSupport {

  case class AppConfig(appName: String,
                       hdfsHost: String,
                       topic: String,
                       hdfsSavePath: String,
                       hdfsTableName: String)

  def run(args: Array[String]): Unit = {
    val Array(
    _,
    postgresHost,
    postgresPort,
    postgresUser,
    postgresPass,
    postgresDB,
    postgresOffsetTable,
    appName,
    brokerUrl,
    topic,
    hdfsHost,
    hdfsSavePath,
    hdfsTableName) = args

    val postgresConfig = PostgresConfig(
      host = postgresHost,
      port = postgresPort.toInt,
      user = postgresUser,
      pass = postgresPass,
      db = postgresDB,
      postgresOffsetTable = postgresOffsetTable)

    val kafkaConfig = KafkaConfig(brokerUrl = brokerUrl)

    val appConfig = AppConfig(
      appName = appName,
      hdfsHost = hdfsHost,
      topic = topic,
      hdfsSavePath = hdfsSavePath,
      hdfsTableName = hdfsTableName)

    runApp(appConfig, postgresConfig, kafkaConfig)
  }

  def runApp(appConfig: AppConfig,
             postgresConfig: PostgresConfig,
             kafkaConfig: KafkaConfig): Unit = {

    log.info(s"Connecting to postgres...")
    PostgresConn.withConn(postgresConfig) { implicit conn =>
      log.info(s"Successfully connected to postgres")

      log.info("Starting spark session")
      SparkRW.withSparkSession(appConfig.appName, appConfig.hdfsHost) { implicit spark =>
        log.info(s"Successfully started spark session")

        startProcessing(kafkaConfig, postgresConfig, appConfig)

      }
    }
    log.info(s"Application successfully finished")
  }

  def startProcessing(kafkaConfig: KafkaConfig,
                      postgresConfig: PostgresConfig,
                      appConfig: AppConfig)(implicit conn: Connection,
                                            spark: SparkSession): Unit = {
    val AppConfig(appName, _, topic, hdfsSavePath, hdfsTableName) = appConfig
    val queryEngine = PostgresQueries(conn, postgresConfig.postgresOffsetTable)
    val run = for {
      previousRunOffset <- queryEngine.getLastOffset(appName = appName, topic = topic)
      lastTopicOffset <- KafkaUtils.getSingleTopicLastOffset(topic = topic, kafkaConfig)
    } yield {
      log.info(s"Comparing startOffset: '$previousRunOffset' > endOffset: '$lastTopicOffset'")
      if (lastTopicOffset > previousRunOffset) {
        val sparkStartOffset = if (previousRunOffset == 0) 0 else previousRunOffset + 1
        val year = Utils.getCurrentYear
        val weekNumber = Utils.getCurrentWeek
        val sparkWritePath = s"$hdfsSavePath/$hdfsTableName/year=$year/week=$weekNumber/"
        for {
          usersDF <- SparkRW.readUserDS(postgresConfig)
          currentRunId = queryEngine.updateOnStart(appName, topic, getTimestampNow, startOffset = sparkStartOffset, endOffset = lastTopicOffset)
          ordersDF <- SparkRW.readTopicBatch(topic = topic, startOffset = previousRunOffset, endOffset = lastTopicOffset, kafkaConfig)
          result <- new OrderWithUserTransformation(orders = ordersDF, users = usersDF).transform
          _ <- SparkRW.writeTopicBatch(df = result, sparkWritePath)
          _ <- queryEngine.updateOnEnd(rowId = currentRunId, endTime = getTimestampNow)
        } yield {}
      } else {
        log.info(s"No new records. Aborting app.")
        Try()
      }
    }

    run.flatten match {
      case Success(_) =>
      case Failure(ex) =>
        log.error(s"Error: $ex")
        throw ex
    }
  }


  class OrderWithUserTransformation(orders: DataFrame,
                                    users: DataFrame) {

    def transform: Try[DataFrame] = Try {
      orders.join(users, Seq("user_id"))
    }

  }

}
