package mx.cinvestav

import fs2.Stream
import cats.implicits._
import cats.effect.IO
import fs2.io.file.Files
import mx.cinvestav.Delcarations.{AppContext, AppContextv2, Trace, baseWriteRequest, readRequest, writeRequest, writeRequestV2}
import mx.cinvestav.MainV3.{SINK_PATH, config}
import org.http4s.{Headers, Response}
import org.http4s.client.Client
import org.typelevel.ci.CIString
import org.typelevel.log4cats.Logger
import retry.{RetryDetails, RetryPolicies, retryingOnFailures}

import java.nio.charset.StandardCharsets
import language.postfixOps
import scala.concurrent.duration._
import java.nio.file.{Path, Paths}
import java.util.UUID
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
object Helpers {

   implicit class StreamPureOps[A](io:IO[A]){
     def pureS = Stream.eval(io)
   }

  def processWriteV3(client: Client[IO])(t:Trace,index:Long=0)(implicit ctx:AppContextv2) = {
    for {
      _              <- ctx.logger.debug(s"START_UPLOAD[$index] ${t.fileId}").pureS
      waitingTime    = t.interArrivalTime.milliseconds
      fileId         = t.fileId
      fileSize       = t.fileSize
      operationId    = t.operationId
      _              <- IO.sleep(waitingTime).pureS
      startAt0       <- IO.monotonic.map(_.toNanos).pureS
      response0      <- client.stream(writeRequestV2(ctx.config.poolUrl)(t))
      endAt0         <- IO.monotonic.map(_.toNanos).pureS
      serviceTime0   = endAt0 - startAt0
      nodeURL        <- response0.as[String].pureS
      startAt1       <- IO.monotonic.map(_.toNanos).pureS
      response1      <- client.stream(baseWriteRequest(nodeURL,t,ctx.config.sourceFolder,ctx.config.staticExtension))
      headers1       = response1.headers
      responseNodeId = headers1.get(CIString("Node-Id")).map(_.head.value).getOrElse(config.nodeId)
      responseLevel  = headers1.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
      endAt1         <- IO.monotonic.map(_.toNanos).pureS
      serviceTime1   = endAt1 - startAt1
      _              <- ctx.logger.info(s"UPLOAD,$responseNodeId,$fileId,$fileSize,$serviceTime0,$serviceTime1,$responseLevel,$operationId").pureS
      _              <- ctx.logger.debug("_____________________________________________").pureS
    } yield ()
  }
  def processWriteV2(client: Client[IO])(t:Trace)(implicit ctx:AppContextv2) = {
    for {
      _           <- ctx.logger.debug(s"START_UPLOAD ${t.fileId}")
      waitingTime = t.interArrivalTime.milliseconds
      _           <- IO.sleep(waitingTime)
      beforeW     <- IO.monotonic.map(_.toNanos)
      response    <- client.stream(writeRequestV2(ctx.config.poolUrl)(t)).evalMap{ response0 =>
        for {
          _ <- IO.unit
          headers0          = response0.headers
//          objectSize        = headers0.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
          isAlreadyUploaded = headers0.get(CIString("Already-Uploaded")).flatMap(x=>x.head.value.toBooleanOption).getOrElse(false)
          res <- if(isAlreadyUploaded) IO.unit
          else for {
            nodeURL <- response0.as[String]
            _       <- client.stream(baseWriteRequest(nodeURL,t,ctx.config.sourceFolder,ctx.config.staticExtension)).evalMap{ response=>
              for {
                _ <- IO.unit
                headers        = response.headers
                afterW         <- IO.monotonic.map(_.toNanos)
                responseNodeId = headers.get(CIString("Node-Id")).map(_.head.value).getOrElse(config.nodeId)
                responseLevel  = headers.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
                _              <- ctx.logger.info(s"UPLOAD,$responseNodeId,${t.fileId},${t.fileSize},${afterW-beforeW},0,$responseLevel,${t.operationId}")
              } yield ()
            }.compile.drain
          } yield ()
        }  yield res
      }.compile.lastOrError
//        .handleError{ e=>
//        ctx.errorLogger.error(e.getMessage) *> IO.unit
//      }
      _ <- ctx.logger.debug("_____________________________________________")
    } yield response
  }
  def processWrite(t:Trace,client: Client[IO])(implicit ctx:AppContext)= for {
    _               <- IO.sleep(t.interArrivalTime milliseconds)
    beforeW         <- IO.monotonic.map(_.toNanos)
    responseHeaders <- client.stream(writeRequest(t)).evalMap{ response =>
      ctx.logger.debug("UPLOAD_RESPONSE_STATUS "+response.status) *> response.headers.pure[IO]
    }.compile
      .last
      .map(_.getOrElse(Headers.empty))
      .onError(e=>ctx.errorLogger.error(e.getMessage))
    responseNodeId = responseHeaders.get(CIString("Node-Id")).map(_.head.value).getOrElse(config.nodeId)
    responseLevel = responseHeaders.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
    afterW         <- IO.monotonic.map(_.toNanos)
    _              <- ctx.logger.info(s"UPLOAD,$responseNodeId,${t.fileId},${t.fileSize},${afterW-beforeW},0,$responseLevel,${t.operationId}")
  } yield ()

  def processDownload(t:Trace,client: Client[IO])(implicit ctx:AppContext)= for {
    _ <- IO.unit
    _            <- IO.sleep(t.interArrivalTime milliseconds)
    beforeW      <- IO.monotonic.map(_.toNanos)
    downloadIO  =  client.toHttpApp.run(readRequest(t))
    res <- retryingOnFailures[Response[IO]](
      wasSuccessful = (a:Response[IO])=>{
        IO.pure(a.status.code ==200)
      },
      onFailure = (a:Response[IO],d:RetryDetails) =>  for {
        _ <- ctx.logger.debug(s"DOWNLOAD_RESPONSE_STATUS ${t.fileId} ${a.status}")
        _ <- ctx.errorLogger.error(d.toString)
      } yield ()
      ,
      policy = RetryPolicies.limitRetries[IO](100) join RetryPolicies.exponentialBackoff[IO](1 second)
    )(downloadIO)
    body        = res.body
    writeS      = body.through(Files[IO].writeAll(Paths.get(SINK_PATH.toString,UUID.randomUUID().toString+s".${config.staticExtension}" ))).compile.drain
    statusPrint = ctx.logger.debug(s"DOWNLOAD_RESPONSE_STATUS ${res.status}")
    responseHeaders  = res.headers
    afterW               <- IO.monotonic.map(_.toNanos)
    responseNodeId       = responseHeaders.get(CIString("Node-Id")).map(_.head.value).getOrElse(config.nodeId)
    responseLevel        = responseHeaders.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
    responseTimeDownload = responseHeaders.get(CIString("Response-Time")).map(_.head.value).getOrElse("0")
    _                    <- ctx.logger.info(s"DOWNLOAD,$responseNodeId,${t.fileId},${t.fileSize},${afterW-beforeW},$responseTimeDownload,$responseLevel,${t.operationId}")
    _                    <- writeS *> statusPrint
    //       _ <- for {
    //    } yield ()
  } yield ()


  def readJsonStr(WORKLOAD_PATH:Path): IO[String] = Files[IO]
    .readAll(WORKLOAD_PATH,8192)
    .compile
    .to(Array)
    .map(new String(_,StandardCharsets.UTF_8))

  def decodeTraces(traceStr:String): IO[List[Trace]] = io.circe.parser.decode[List[Trace]](traceStr) match {
    case Left(e) => IO.pure(List.empty[Trace])
    case Right(trace) => trace.pure[IO]

  }


  private def spin(milliseconds: Int): Unit = {
    val sleepTime = milliseconds * 1000000L // convert to nanoseconds
    val startTime = System.nanoTime
    while ( {
      (System.nanoTime - startTime) < sleepTime
    }) {
    }
  }

}
