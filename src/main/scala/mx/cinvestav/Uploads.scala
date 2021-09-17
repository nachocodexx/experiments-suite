package mx.cinvestav

import cats.effect.{ExitCode, IO, IOApp}
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AmqpMessage, AmqpProperties, ExchangeName, QueueName, RoutingKey}
import fs2.io.file.Files
import fs2.text

import java.nio.file.Paths
import mx.cinvestav.Delcarations.Upload
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.utils.RabbitMQUtils
import mx.cinvestav.utils.v2.{Exchange, MessageQueue, PublisherConfig, PublisherV2, RabbitMQContext}
import pureconfig._
import pureconfig.generic.auto._
import mx.cinvestav.commons.payloads.{v2 => PAYLOADS}
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.utils.v2.encoders._
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import concurrent.duration._
import language.postfixOps

object Uploads  extends IOApp{
  final val TARGET = Paths.get("/home/nacho/Programming/Scala/experiments-suite/target")
  final val WORKLOAD_FILE = "uploads_0.csv"
  final val WORKLOAD_PATH = TARGET.resolve("workloads").resolve(WORKLOAD_FILE)
  implicit val config:DefaultConfig = ConfigSource.default.loadOrThrow[DefaultConfig]
  implicit val rabbitMQConfig = RabbitMQUtils.parseRabbitMQClusterConfig(config.rabbitmq)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  //      REPLICATION FACTORS
  val replicationFactors = Map(
    "Alpha" ->3,
    "Beta" -> 1,
    "Gamma"-> 1
  )


  def program(n:Int=100) = {
    Files[IO].readAll(WORKLOAD_PATH,4092)
      .through(text.utf8Decode)
      .through(text.lines)
      .tail
      .metered(5 seconds)
      .take(n)
      .map(_.split(","))
      .filter(_.nonEmpty)
      .map(x=>Upload(userId=x(0),operation=x(1),fileId = x(2),fileSize = x(3).toDouble,userRole = x(4)))
//      .debug()
  }

  def updateUserRF(u:Upload)= {
    val properties = AmqpProperties( headers = Map("commandId"->StringVal("UPDATE_USER_RF")) )
    val replicationFactor = replicationFactors(u.userRole)
    val payload    = PAYLOADS.UpdateUserRF(u.userId,replicationFactor)
    val msg        = AmqpMessage[String](payload=payload.asJson.noSpaces,properties = properties)

  }
//
  override def run(args: List[String]): IO[ExitCode] = {
    RabbitMQUtils.initV2[IO](rabbitMQConfig){ implicit client =>
      client.createConnection.use { implicit connection =>
        implicit val rabbitMQContext = RabbitMQContext(connection = connection,client = client)
        val dataPrepCfg = PublisherConfig(exchangeName = ExchangeName("data_prep"),routingKey = RoutingKey("dp-0"))
        val lbCfg = PublisherConfig(exchangeName = ExchangeName("load_balancer"),routingKey = RoutingKey("load_balancer.lb-0"))
        val dataPreparationPub = PublisherV2(publisherConfig = dataPrepCfg)
        val lbPub = PublisherV2(publisherConfig = lbCfg)
//       _________________________________________________________
        client.createChannel(connection).use{ implicit channel =>
          program()
            .evalMap{ upload =>
              val userId   = upload.userId
              val userRole = upload.userRole
              val fileId   = upload.fileId
              val rf       = replicationFactors(userRole)
              val properties = AmqpProperties(headers = Map("commandId"->StringVal("UPDATE_USER_RF")))
              val payload    = PAYLOADS.UpdateUserRF(userId = userId,replicationFactor = rf)
              val msg       = AmqpMessage[String](payload = payload.asJson.noSpaces,properties = properties)
              for {
                _ <-Logger[IO].debug(s"UPDATE_USER_RF $userId $fileId $rf")
                _ <- lbPub.publish(msg)
              } yield (upload)
            }
            .evalMap{ upload=>
              val userId   = upload.userId
              val userRole = upload.userRole
              val fileId   = upload.fileId
              val properties = AmqpProperties(headers = Map("commandId"->StringVal("SLICE_COMPRESS")))
              val payload    = PAYLOADS.SliceAndCompress(url = s"http://69.0.0.2/${upload.fileId}.txt",chunkSize = 64000000,userId=upload.userId,"LZ4")
              val msg        = AmqpMessage[String](payload=payload.asJson.noSpaces,properties=properties)
              for {
                _ <- Logger[IO].debug(s"UPLOAD_FILE $userId $fileId $userRole")
                _ <- dataPreparationPub.publish(msg)
              } yield ()
//              IO.unit
            }
            .compile.drain
        }
      }

    }
      .as(ExitCode.Success)
  }
//
}
