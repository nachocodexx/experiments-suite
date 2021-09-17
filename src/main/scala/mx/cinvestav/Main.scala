package mx.cinvestav

import cats.effect.kernel.Resource
import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp, Ref}
import com.rabbitmq.client.AMQP.Exchange
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.model.ExchangeType
import fs2.Stream
import io.circe.Json
import mx.cinvestav.commons.payloads
import mx.cinvestav.commons.commands.Identifiers
import mx.cinvestav.commons.balancer
import mx.cinvestav.commons.commands.CommandData
import mx.cinvestav.config.{CacheNode, DefaultConfig}
import mx.cinvestav.utils.RabbitMQUtils
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import pureconfig.ConfigSource

import java.io.File
import scala.concurrent.duration._
import scala.language.postfixOps
import java.util.UUID
import pureconfig.generic.auto._
import pureconfig.ConfigSource
import io.circe._
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import io.circe.syntax._
import io.circe.parser._
import fs2.io.file.Files
import fs2.text
import io.circe.syntax._
import io.circe.generic.auto._
import org.http4s.{Header, Headers, HttpVersion, MediaType, Method, Request, Uri, headers}
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client
import org.http4s.multipart.{Multipart, Part}

import java.nio.file.{Path, Paths}
import scala.concurrent.ExecutionContext.global
import scala.tools.nsc

object Main extends IOApp{
  implicit val config: DefaultConfig  = ConfigSource.default.loadOrThrow[DefaultConfig]
  val clientResource: Resource[IO, Client[IO]] = BlazeClientBuilder[IO](global).resource
  val workloadPath: Path              = Paths.get(config.workloadPath+"/"+config.workloadFilename)
  val rabbitMQConfig: Fs2RabbitConfig = RabbitMQUtils.parseRabbitMQClusterConfig(config.rabbitmq)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  case class CacheWorkload(userId:String,operation:String,fileId:String,arrivalTime:Int,fileSize:Int,user2Id:String)

  def workload[A](workloadPath:Path,mapFn:List[String]=>A): Stream[IO,A] = {
  Files[IO].readAll(workloadPath,4092)
    .through(text.utf8Decode)
    .through(text.lines)
    .tail
    .map(_.split(","))
    .filter(_.length>=3)
    .map(_.toList)
    .map(mapFn)
  }



  val writeReq:(CacheNode,File) => Request[IO] = (node,file) => {
    val multipart = Multipart[IO](
      parts = Vector(
        Part.fileData(
          "upload",
          file,
          headers = Headers(
            headers.`Content-Type`(MediaType.application.pdf),
          )
        )
      )
    )
    val req = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(s"${node.url}/upload"),
//      httpVersion = HttpVersion.`HTTP/1.1`,
      headers = multipart.headers
    ).withEntity(multipart)
    req
//      .putHeaders(multipart.headers)
  }

  val readReq:(CacheNode,String) => Request[IO] = (node,fileId)=> {
    val req = Request[IO](
      method = Method.GET,
      uri = Uri.unsafeFromString(s"${node.url}/download?file=${fileId}.${config.staticExtension}"),
//      httpVersion = HttpVersion.`HTTP/1.1`,
    )
    req
  }

  override def run(args: List[String]): IO[ExitCode] ={
//    val cacheWorkloadPath:Path = Paths.get("")
    var lastArrivalTime = Int.MinValue
    val app = for{
      _ <- Logger[IO].debug(config.toString)
      (client,finalizer) <- clientResource.allocated
      cacheNode         = config.cacheNodes.head
      _ <- workload[CacheWorkload](workloadPath,x=>CacheWorkload(x.head,x(1),x(2),x(3).toInt,x(4).toInt,x(5)))
      .evalMap{ row=>
        if(row.operation === "w") for {
          _  <- Logger[IO].info(s"UPLOAD ${row.userId} ${row.operation} ${row.fileId} ${row.fileSize} ${row.user2Id}")
          waitingTime0 = row.arrivalTime - lastArrivalTime
          waitingTime  = if(waitingTime0 < 0 ) 0 else waitingTime0
          _            <- IO.delay{lastArrivalTime = row.arrivalTime}
          _            <- Logger[IO].info(s"WAITING_TIME $waitingTime")
          file         = new File(s"${config.sourceFolder}/${row.fileId}.${config.staticExtension}")
          _            <- Logger[IO].info(s"FILE_EXISTS ${row.fileId} ${file.exists()}")
          _            <- IO.sleep(waitingTime milliseconds)
          response     <- client.expectOptionOr[String](writeReq(cacheNode,file)){ response =>
            IO.pure(new Throwable(response.status.reason)) <* Logger[IO].error(response.status.reason)
          }
          _            <- Logger[IO].debug(response.toString)
        } yield ()
        else  for {
          _  <- Logger[IO].info(s"DOWNLOAD ${row.userId} ${row.operation} ${row.fileId} ${row.fileSize} ${row.user2Id}")
          waitingTime0 = row.arrivalTime - lastArrivalTime
          waitingTime  = if(waitingTime0 < 0 ) 0 else waitingTime0
          _            <- IO.delay{lastArrivalTime = row.arrivalTime}
          _            <- Logger[IO].info(s"WAITING_TIME $waitingTime")
          _            <- IO.sleep(waitingTime milliseconds)
          response     <- client.expectOptionOr[String](readReq(cacheNode,row.fileId)){ response =>
            IO.pure(new Throwable(response.status.reason)) <* Logger[IO].error(response.status.reason)
          }
          _            <- Logger[IO].debug(response.toString)
//          _            <- Logger[IO].debug(s"STATUS ${response}")
        } yield ()
      }
        .compile
        .drain
      _                  <- finalizer
    } yield ()
      app.as(ExitCode.Success)
  }
}
