package com.test_task

import com.test_task.spark.SparkApp
import com.test_task.producer.KafkaProducerApp


object MainApp {

  def main(args: Array[String]): Unit = {
    val mode = args(0)
    mode match {
      case "producer" =>
        KafkaProducerApp.run(args)
      case "spark" =>
        SparkApp.run(args)
    }
  }

}
