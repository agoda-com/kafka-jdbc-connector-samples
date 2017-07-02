package com.agoda.kafka.connect

import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._

class Settings {
  private val config = ConfigFactory.load()

  lazy val kafkaBrokers: List[String] = config.getStringList("kafka.brokers").asScala.toList

  lazy val workerHost: String = config.getString("kafka.worker.rest-host")

  lazy val workerPort: Int = config.getInt("kafka.worker.rest-port")

  lazy val workerGroup: String = config.getString("kafka.worker.group")

  lazy val workerConfigStorage: String = config.getString("kafka.worker.config-storage")

  lazy val workerOffsetStorage: String = config.getString("kafka.worker.offset-storage")
}
