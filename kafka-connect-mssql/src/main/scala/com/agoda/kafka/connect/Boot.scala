package com.agoda.kafka.connect

import akka.actor.ActorSystem
import com.agoda.kafka.connect.WorkerManager.StartWorker

import scala.concurrent.ExecutionContext
import scala.language.postfixOps

object Boot extends App {

  def run()(implicit system: ActorSystem, ec: ExecutionContext): Unit = {
    val assembly = new Assembly
    import assembly._

    workerManager ! StartWorker
  }

  override def main(args: Array[String]): Unit = {
    implicit val actorSystem = ActorSystem("kafka-connect")
    implicit val executor = actorSystem.dispatcher
    run()
  }
}