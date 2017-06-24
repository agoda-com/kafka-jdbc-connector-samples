package com.agoda.kafka.connect

import java.sql.{Connection, DriverManager, Timestamp}
import java.util.{Calendar, TimeZone}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.reactivestreams.Publisher
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationDouble}
import scala.language.postfixOps
import scalaj.http.Http

class KafkaConnectMssqlFunctionalTest extends WordSpec with Matchers with BeforeAndAfterAll {
  var db: Connection = _
  var publisherTimestamp: Publisher[ConsumerRecord[String, String]] = _
  var publisherIncrementing: Publisher[ConsumerRecord[String, String]] = _
  var publisherTimestampIncrementing: Publisher[ConsumerRecord[String, String]] = _

  implicit val system = ActorSystem("functional-test")
  implicit val materializer = ActorMaterializer()

  "Kafka Connect in Timestamp mode" should {
    "should stream data into kafka" in {
      val url = "http://localhost:8083/connectors"
      val data = """
                  |{
                  |	"name" : "timestamp",
                  |	"config" : {
                  |		"tasks.max": "1",
                  |		"connector.class": "com.agoda.kafka.connector.jdbc.JdbcSourceConnector",
                  |		"connection.url" : "jdbc:sqlserver://mssql:1433;databaseName=tempdb;user=sa;password=Passw0rd",
                  |		"batch.max.rows.variable.name" : "batch",
                  |		"batch.max.rows" : "2",
                  |		"mode" : "timestamp",
                  |		"timestamp.variable.name" : "time",
                  |		"timestamp.field.name" : "change_timestamp",
                  |		"timestamp.offset" : "2017-03-13 05:33:57.000",
                  |		"stored-procedure.name" : "sp_cdc_timestamp",
                  |		"topic" : "test-timestamp",
                  |		"key.field.name" : "id"
                  |	}
                  |}""".stripMargin

      Http(url).postData(data).header("content-type", "application/json").asString.code

      val messages = Await.result(
        Source.fromPublisher(publisherTimestamp)
          .map(m => s"${m.key()} => ${m.value()}")
          .take(5)
          .takeWithin(1.minute)
          .runWith(Sink.seq[String]),
        Duration.Inf
      )

      messages shouldEqual ListBuffer(
        """6 => {"id":6,"change_timestamp":1489383241000}""",
        """7 => {"id":7,"change_timestamp":1489383242000}""",
        """8 => {"id":8,"change_timestamp":1489383244000}""",
        """9 => {"id":9,"change_timestamp":1489383246000}""",
        """10 => {"id":10,"change_timestamp":1489383247000}"""
      )
    }
  }

  "Kafka Connect in Incrementing mode" should {
    "should stream data into kafka" in {
      val url = "http://localhost:8083/connectors"
      val data = """
                  |{
                  |	"name" : "incrementing",
                  |	"config" : {
                  |		"tasks.max": "1",
                  |		"connector.class": "com.agoda.kafka.connector.jdbc.JdbcSourceConnector",
                  |		"connection.url" : "jdbc:sqlserver://mssql:1433;databaseName=tempdb;user=sa;password=Passw0rd",
                  |		"batch.max.rows.variable.name" : "batch",
                  |		"batch.max.rows" : "2",
                  |		"mode" : "incrementing",
                  |		"incrementing.variable.name" : "id",
                  |		"incrementing.field.name" : "id",
                  |		"incrementing.offset" : "7",
                  |		"stored-procedure.name" : "sp_cdc_incrementing",
                  |		"topic" : "test-incrementing",
                  |		"key.field.name" : "id"
                  |	}
                  |}""".stripMargin

      Http(url).postData(data).header("content-type", "application/json").asString.code

      val messages = Await.result(
        Source.fromPublisher(publisherIncrementing)
          .map(m => s"${m.key()} => ${m.value()}")
          .take(3)
          .takeWithin(1.minute)
          .runWith(Sink.seq[String]),
        Duration.Inf
      )

      messages shouldEqual ListBuffer(
        """8 => {"id":8,"change_timestamp":1489383244000}""",
        """9 => {"id":9,"change_timestamp":1489383246000}""",
        """10 => {"id":10,"change_timestamp":1489383247000}"""
      )
    }
  }

  "Kafka Connect in Timestamp+Incrementing mode" should {
    "should stream data into kafka" in {
      val url = "http://localhost:8083/connectors"
      val data = """
                   |{
                   |	"name" : "timestamp_incrementing",
                   |	"config" : {
                   |		"tasks.max": "1",
                   |		"connector.class": "com.agoda.kafka.connector.jdbc.JdbcSourceConnector",
                   |		"connection.url" : "jdbc:sqlserver://mssql:1433;databaseName=tempdb;user=sa;password=Passw0rd",
                   |		"batch.max.rows.variable.name" : "batch",
                   |		"batch.max.rows" : "2",
                   |		"mode" : "timestamp+incrementing",
                   |    "timestamp.variable.name" : "time",
                   |    "timestamp.field.name" : "change_timestamp",
                   |    "timestamp.offset" : "2017-03-13 05:33:51.000",
                   |		"incrementing.variable.name" : "id",
                   |		"incrementing.field.name" : "id",
                   |		"incrementing.offset" : "3",
                   |		"stored-procedure.name" : "sp_cdc_timestamp_incrementing",
                   |		"topic" : "test-timestamp-incrementing",
                   |		"key.field.name" : "id"
                   |	}
                   |}""".stripMargin

      Http(url).postData(data).header("content-type", "application/json").asString.code

      val messages = Await.result(
        Source.fromPublisher(publisherTimestampIncrementing)
          .map(m => s"${m.key()} => ${m.value()}")
          .take(7)
          .takeWithin(1.minute)
          .runWith(Sink.seq[String]),
        Duration.Inf
      )

      messages shouldEqual ListBuffer(
        """4 => {"id":4,"change_timestamp":1489383231000}""",
        """5 => {"id":5,"change_timestamp":1489383237000}""",
        """6 => {"id":6,"change_timestamp":1489383241000}""",
        """7 => {"id":7,"change_timestamp":1489383242000}""",
        """8 => {"id":8,"change_timestamp":1489383244000}""",
        """9 => {"id":9,"change_timestamp":1489383246000}""",
        """10 => {"id":10,"change_timestamp":1489383247000}"""
      )
    }
  }

  lazy private val dropTable   = db.prepareStatement("DROP TABLE IF EXISTS dbo.test")
  lazy private val createTable = db.prepareStatement("CREATE TABLE test (id INT, change_timestamp DATETIME)")
  lazy private val insertData  = db.prepareStatement(
    """
      |INSERT INTO TEST VALUES(1,  ?);
      |INSERT INTO TEST VALUES(2,  ?);
      |INSERT INTO TEST VALUES(3,  ?);
      |INSERT INTO TEST VALUES(4,  ?);
      |INSERT INTO TEST VALUES(5,  ?);
      |INSERT INTO TEST VALUES(6,  ?);
      |INSERT INTO TEST VALUES(7,  ?);
      |INSERT INTO TEST VALUES(8,  ?);
      |INSERT INTO TEST VALUES(9,  ?);
      |INSERT INTO TEST VALUES(10, ?);
      |""".stripMargin
  )
  lazy private val dropTimestampProcedure = db.prepareStatement(
    """
      |IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'sp_cdc_timestamp') AND type in (N'P', N'PC'))
      | DROP PROCEDURE sp_cdc_timestamp
    """.stripMargin
  )
  lazy private val createTimestampProcedure = db.prepareStatement(
    """
      |CREATE PROCEDURE sp_cdc_timestamp (@time DATETIME, @batch INT)
      |AS
      |BEGIN
      |	SELECT TOP (@batch) *
      |	FROM tempdb.dbo.test
      |	WHERE change_timestamp > @time
      |	ORDER BY change_timestamp ASC
      |END
    """.stripMargin
  )
  lazy private val dropIncrementingProcedure = db.prepareStatement(
    """
      |IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'sp_cdc_incrementing') AND type in (N'P', N'PC'))
      | DROP PROCEDURE sp_cdc_incrementing
    """.stripMargin
  )
  lazy private val createIncrementingProcedure = db.prepareStatement(
    """
      |CREATE PROCEDURE sp_cdc_incrementing (@id BIGINT, @batch INT)
      |AS
      |BEGIN
      |	SELECT TOP (@batch) *
      |	FROM tempdb.dbo.test
      |	WHERE id > @id
      |	ORDER BY id ASC
      |END
    """.stripMargin
  )
  lazy private val dropTimestampIncrementingProcedure = db.prepareStatement(
    """
      |IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'sp_cdc_timestamp_incrementing') AND type in (N'P', N'PC'))
      | DROP PROCEDURE sp_cdc_timestamp_incrementing
    """.stripMargin
  )
  lazy private val createTimestampIncrementingProcedure = db.prepareStatement(
    """
      |CREATE PROCEDURE sp_cdc_timestamp_incrementing (@time DATETIME, @id BIGINT, @batch INT)
      |AS
      |BEGIN
      |	SELECT TOP (@batch) *
      |	FROM tempdb.dbo.test
      |	WHERE
      |		(change_timestamp > @time)
      |		OR
      |		(change_timestamp = @time AND id > @id)
      |	ORDER BY change_timestamp, id ASC
      |END
    """.stripMargin
  )
  lazy private val kafka        = new ReactiveKafka()
  lazy private val deserializer = new StringDeserializer()

  override def beforeAll(): Unit = {
    val dbUrl = "jdbc:sqlserver://localhost:1433;databaseName=tempdb;user=sa;password=Passw0rd"
    db = DriverManager.getConnection(dbUrl)
    val UTC_Calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    insertData.setTimestamp(1,  new Timestamp(1489383225000L), UTC_Calendar)
    insertData.setTimestamp(2,  new Timestamp(1489383229000L), UTC_Calendar)
    insertData.setTimestamp(3,  new Timestamp(1489383231000L), UTC_Calendar)
    insertData.setTimestamp(4,  new Timestamp(1489383231000L), UTC_Calendar)
    insertData.setTimestamp(5,  new Timestamp(1489383237000L), UTC_Calendar)
    insertData.setTimestamp(6,  new Timestamp(1489383241000L), UTC_Calendar)
    insertData.setTimestamp(7,  new Timestamp(1489383242000L), UTC_Calendar)
    insertData.setTimestamp(8,  new Timestamp(1489383244000L), UTC_Calendar)
    insertData.setTimestamp(9,  new Timestamp(1489383246000L), UTC_Calendar)
    insertData.setTimestamp(10, new Timestamp(1489383247000L), UTC_Calendar)
    dropTable.execute()
    createTable.execute()
    insertData.execute()
    dropTimestampProcedure.execute()
    createTimestampProcedure.execute()
    dropIncrementingProcedure.execute()
    createIncrementingProcedure.execute()
    dropTimestampIncrementingProcedure.execute()
    createTimestampIncrementingProcedure.execute()

    publisherTimestamp             = kafka.consume(ConsumerProperties("kafka:9092", "test-timestamp", "functional-test", deserializer, deserializer))
    publisherIncrementing          = kafka.consume(ConsumerProperties("kafka:9092", "test-incrementing", "functional-test", deserializer, deserializer))
    publisherTimestampIncrementing = kafka.consume(ConsumerProperties("kafka:9092", "test-timestamp-incrementing", "functional-test", deserializer, deserializer))
  }

  override def afterAll(): Unit = db.close()
}
