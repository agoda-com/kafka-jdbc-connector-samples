package com.agoda.kafka.connect

import akka.actor.{ActorRef, ActorSystem, Props}
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.runtime.distributed.{DistributedConfig, DistributedHerder}
import org.apache.kafka.connect.runtime.rest.RestServer
import org.apache.kafka.connect.runtime.{Connect, Worker, WorkerConfig}
import org.apache.kafka.connect.storage.{KafkaConfigStorage, KafkaOffsetBackingStore, StringConverter}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class Assembly(implicit system: ActorSystem, ec: ExecutionContext)  {
  private val settings = new Settings

  private val workerProps = Map(
    WorkerConfig.BOOTSTRAP_SERVERS_CONFIG               -> settings.kafkaBrokers.mkString(","),
    DistributedConfig.GROUP_ID_CONFIG                   -> settings.workerGroup,
    WorkerConfig.REST_HOST_NAME_CONFIG                  -> settings.workerHost,
    WorkerConfig.REST_PORT_CONFIG                       -> settings.workerPort.toString,
    KafkaConfigStorage.CONFIG_TOPIC_CONFIG              -> settings.workerConfigStorage,
    KafkaOffsetBackingStore.OFFSET_STORAGE_TOPIC_CONFIG -> settings.workerOffsetStorage,
    WorkerConfig.KEY_CONVERTER_CLASS_CONFIG             -> classOf[StringConverter].getCanonicalName,
    WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG    -> classOf[StringConverter].getCanonicalName,
    WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG           -> classOf[JsonConverter].getCanonicalName,
    WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG  -> classOf[JsonConverter].getCanonicalName,
    "internal.key.converter.schemas.enable"             -> false.toString,
    "internal.value.converter.schemas.enable"           -> false.toString,
    "key.converter.schemas.enable"                      -> false.toString,
    "value.converter.schemas.enable"                    -> false.toString
  )

  val distributedWorkerConfig = new DistributedConfig(workerProps.asJava)
  val workerOffsetStorage     = new KafkaOffsetBackingStore
  val worker                  = new Worker(distributedWorkerConfig, workerOffsetStorage)
  val restServer              = new RestServer(distributedWorkerConfig)
  val distributedHerder       = new DistributedHerder(distributedWorkerConfig, worker, restServer.advertisedUrl)
  val connect                 = new Connect(worker, distributedHerder, restServer)
  val workerManager: ActorRef = system.actorOf(Props(new WorkerManager(connect)))
}
