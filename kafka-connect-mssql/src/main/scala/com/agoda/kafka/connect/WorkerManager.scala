package com.agoda.kafka.connect

import akka.actor.Actor
import org.apache.kafka.connect.runtime.Connect

class WorkerManager(connect: Connect) extends Actor {

  import WorkerManager._

  override def receive: Receive = {
    case StartWorker => connect.start()
    case StopWorker  => connect.stop()
  }
}

object WorkerManager {
  case object StartWorker
  case object StopWorker
}
