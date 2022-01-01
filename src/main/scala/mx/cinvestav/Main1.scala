package mx.cinvestav

import cats.effect.kernel.Resource
import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp, Ref}
import fs2.Stream
import mx.cinvestav.config.{CacheNode, DefaultConfig}
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
import org.typelevel.ci.CIString

import java.nio.file.{Path, Paths}
import scala.concurrent.ExecutionContext.global
import scala.tools.nsc

object Main1 extends IOApp{
  implicit val config: DefaultConfig  = ConfigSource.default.loadOrThrow[DefaultConfig]
  val clientResource: Resource[IO, Client[IO]] = BlazeClientBuilder[IO](global)
    .withRequestTimeout(10 minutes)
    .resource
  val workloadPath: Path              = Paths.get(config.workloadPath+"/"+config.workloadFilename)
//  val rabbitMQConfig: Fs2RabbitConfig = RabbitMQUtils.parseRabbitMQClusterConfig(config.rabbitmq)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  case class CacheWorkload(userId:String,operation:String,fileId:String,arrivalTime:Int,fileSize:Int,user2Id:String)
  val sinkPath     = Paths.get(config.sinkFolder)
  val globalUserId = UUID.fromString("251da30f-7a6b-485a-a1a6-855ea0434680")

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

  val writeReq2:(UUID,String,File,CacheWorkload) => Request[IO] = (operationId,lbURL,file,row) => {
    val multipart = Multipart[IO](
      parts = Vector(
        Part.fileData(
          "upload",
          file,
          headers = Headers(
            Header.Raw(CIString("Object-Id"),row.fileId),
            headers.`Content-Type`(MediaType.text.plain),
            headers.`Content-Length`(file.length())
          )
        )
      )
    )
    val req = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(s"$lbURL/upload"),
      headers = multipart.headers
    )
      .withEntity(multipart)
      .putHeaders(
        Headers(
          Header.Raw(CIString("Operation-Id"),operationId.toString),
//          Header.Raw(CIString("User-Id"),globalUserId.toString),
          Header.Raw(CIString("User-Id"),row.userId),
          Header.Raw(CIString("Bucket-Id"), "nacho-bucket" ),
          Header.Raw(CIString("Object-Id"),row.fileId),
          Header.Raw(CIString("Object-Size"),file.length().toString),
          //          Header.Raw(CIString("Compression-Algorithm"),"LZ4")
        )
      )
    req
  }

  val readReq:(UUID,String,File,CacheWorkload) => Request[IO] = (operationId,node,file,row)=> {
    val req = Request[IO](
      method = Method.GET,
      uri = Uri.unsafeFromString(s"$node/download/${row.fileId}"),
      headers = Headers(
        Header.Raw(CIString("Operation-Id"),operationId.toString),
        Header.Raw(CIString("User-Id"),row.user2Id),
        Header.Raw(CIString("Bucket-Id"),"default"),
        Header.Raw(CIString("Object-Size"),file.length().toString),
        Header.Raw(CIString("Object-Extension"),config.staticExtension),
      )
    )
    req
  }

  override def run(args: List[String]): IO[ExitCode] ={
//    val cacheWorkloadPath:Path = Paths.get("")
    var lastArrivalTime = Int.MinValue
    var operationCounter:Int = config.drop
    val app = for{
      _ <- Logger[IO].debug(config.toString)
      (client,finalizer) <- clientResource.allocated
//      cacheNode         = "http://localhost:3000/api/v6"
      cacheNode         = config.poolUrl
      _ <- workload[CacheWorkload](workloadPath,x=>CacheWorkload(x.head,x(1),x(2),x(3).toInt,x(4).toInt,x(5)))
//        .drop(config.drop)
      .evalMap{ row=>
        for {
          _ <- IO.unit
          operationId  = UUID.randomUUID()
          _ <- if(row.operation === "w") for {
            _ <- IO.unit
            file         = new File(s"${config.sourceFolder}/${row.fileId}.${config.staticExtension}")
            waitingTime0 = row.arrivalTime - lastArrivalTime
            waitingTime  = if(waitingTime0 < 0 ) 0 else waitingTime0
            _            <- IO.delay{lastArrivalTime = row.arrivalTime}
            //          _            <- Logger[IO].e(s"WAITING_TIME,${row.fileId},$waitingTime")
            _            <- Logger[IO].debug(s"FILE_EXISTS ${row.fileId} ${file.exists()}")
            _            <- IO.sleep(waitingTime milliseconds)
            beforeW      <- IO.monotonic.map(_.toNanos)
            responseHeaders     <- client.stream(writeReq2(operationId,cacheNode,file,row)).evalMap{ response =>
                Logger[IO].debug("UPLOAD_RESPONSE_STATUS "+response.status) *> response.headers.pure[IO]
              }.compile.last.map(_.getOrElse(Headers.empty))
            responseNodeId = responseHeaders.get(CIString("Node-Id")).map(_.head.value).getOrElse(config.nodeId)
            responseLevel = responseHeaders.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
            afterW         <- IO.monotonic.map(_.toNanos)
            _              <- Logger[IO].info(s"UPLOAD,$responseNodeId,${row.fileId},${row.fileSize},${afterW-beforeW},0,$responseLevel,$operationId")
            } yield ()
            else  for {
              _            <- IO.unit
              file         = new File(s"${config.sourceFolder}/${row.fileId}.${config.staticExtension}")
              waitingTime0 = row.arrivalTime - lastArrivalTime
              waitingTime  = if(waitingTime0 < 0 ) 0 else waitingTime0
              _            <- IO.delay{lastArrivalTime = row.arrivalTime}
              _            <- IO.sleep(waitingTime milliseconds)
              beforeW      <- IO.monotonic.map(_.toNanos)
              _  <- client.stream(readReq(operationId,cacheNode,file,row)).evalMap{ res=>
                val body        = res.body
                val writeS      = body.through(Files[IO].writeAll(Paths.get(sinkPath.toString,UUID.randomUUID().toString+s".${config.staticExtension}" ))).compile.drain
                val statusPrint = Logger[IO].debug(s"DOWNLOAD_RESPONSE_STATUS ${res.status}")
                val responseHeaders  = res.headers
                for {
                  afterW               <- IO.monotonic.map(_.toNanos)
                  responseNodeId       = responseHeaders.get(CIString("Node-Id")).map(_.head.value).getOrElse(config.nodeId)
                  responseLevel        = responseHeaders.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
                  responseTimeDownload = responseHeaders.get(CIString("Response-Time")).map(_.head.value).getOrElse("0")
                  _                    <- Logger[IO].info(s"DOWNLOAD,$responseNodeId,${row.fileId},${row.fileSize},${afterW-beforeW},$responseTimeDownload,$responseLevel,$operationId")
                  _                    <- writeS *> statusPrint
                } yield ()
              }.compile.drain
//                last.map(_.getOrElse(Headers.empty))
            } yield ()
          _ <- Logger[IO].debug(s"OPERATION[$operationCounter] COMPLETED")
          _ <- IO.delay{operationCounter+=1}
          _ <- Logger[IO].debug(s"____________________________________________________________________")
        } yield ()
//
      }
        .compile
        .drain
      _                  <- finalizer
    } yield ()
      app.as(ExitCode.Success)
  }
}
