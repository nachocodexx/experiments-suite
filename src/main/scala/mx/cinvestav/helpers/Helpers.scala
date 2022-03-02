package mx.cinvestav.helpers

import cats.effect.IO
import cats.implicits._
import fs2.Stream
import fs2.io.file.Files
import io.circe.generic.auto._
import mx.cinvestav.Delcarations._
import org.http4s.client.Client
import org.http4s.{Headers, Response}
import org.typelevel.ci.CIString
import retry.{RetryDetails, RetryPolicies, retryingOnFailures}

import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}
import java.util.UUID
//
import scala.concurrent.duration._
import scala.language.postfixOps

object Helpers {

   implicit class StreamPureOps[A](io:IO[A]){
     def pureS = Stream.eval(io)
   }

  def toURL(u:String)(implicit ctx:AppContextv2) = {
      val url = new URL(u)
      (new URL(url.getProtocol,if(ctx.config.appMode=="LOCAL") "localhost" else url.getHost,url.getPort,url.getPath)).toString
  }

  def processWriteV3(client: Client[IO])(t:Trace,initTime:Long,index:Long=0)(implicit ctx:AppContextv2) = {
    for {
      _                <- IO.sleep(ctx.config.producerRate milliseconds).pureS
      getNanoTime      = IO.monotonic.map(_.toNanos).map(_ - initTime)
      //    ________________________________________________________
      serviceTimeStart <- getNanoTime.pureS
      arrivalTime      = t.arrivalTime
      objectId         = t.fileId
      objectSize       = t.fileSize
      operationId      = t.operationId
      //    _____________________________________________________________
      response0         <- client.stream(writeRequestV2(ctx.config.poolUrl)(t))
      headers0          = response0.headers
      waitingTime0      = headers0.get(CIString("Waiting-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
      serviceTime0      = headers0.get(CIString("Service-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
      serviceTimeStart0 = headers0.get(CIString("Service-Time-Start")).flatMap(_.head.value.toLongOption).getOrElse(0L)
      serviceTimeEnd0   = headers0.get(CIString("Service-Time-End")).flatMap(_.head.value.toLongOption).getOrElse(0L)
      _                <- ctx.logger.debug(s"RESPONSE0_STATUS $response0").pureS
      selectedNodeURL  <- response0.as[String].pureS.evalMap(x=> Helpers.toURL(x).pure[IO])
      _                <- ctx.logger.debug(s"SELECTED_NODE $selectedNodeURL").pureS
      endAt0           <- getNanoTime.pureS
//        IO.monotonic.map(_.toNanos).pureS
      responseTime0    = endAt0 - serviceTimeStart
      //    ___________________________________________________________
      startAt1        <- getNanoTime.pureS
//        IO.monotonic.map(_.toNanos).pureS
      sourceFolder    = ctx.config.sourceFolder
      staticExtension = ctx.config.staticExtension
      req1            = baseWriteRequest(selectedNodeURL,t,sourceFolder,staticExtension)
//      x = client
      request1IO      = client
        .stream(req1)
        .flatTap(
            x=>ctx.logger.debug(s"REQUEST1_STATUS ${x.status}").pureS
        )
        .flatMap { response1 =>
          for {
            endAt1         <- getNanoTime.pureS
            responseTime1  = endAt1 - startAt1
            _              <- ctx.logger.debug(s"RESPONSE1 $response1").pureS
            headers1       = response1.headers
            selectedNodeId = headers1.get(CIString("Node-Id")).map(_.head.value).getOrElse(ctx.config.nodeId)
            responseLevel  = headers1.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
//              IO.monotonic.map(_.toNanos).pureS
            //   _____________________________________________________
            //
            waitingTime1      = headers1.get(CIString("Waiting-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
            serviceTime1      = headers1.get(CIString("Service-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
            serviceTimeStart1 = headers1.get(CIString("Service-Time-Start")).flatMap(_.head.value.toLongOption).getOrElse(0L)
            serviceTimeEnd1   = headers1.get(CIString("Service-Time-End")).flatMap(_.head.value.toLongOption).getOrElse(0L)
            //
            serviceTimeEnd    <- getNanoTime.pureS
//              IO.monotonic.map(_.toNanos).map(_ - initTime).pureS
            serviceTime       = serviceTimeEnd - serviceTimeStart
            //            waitingTime       =  serviceTimeStart - arrivalTime
            _                 <- ctx.logger.info(
              s"UPLOAD,$operationId,$responseLevel,$objectId,$objectSize,$selectedNodeId,$responseTime0,$responseTime1,$serviceTimeStart0,$serviceTimeEnd0,$serviceTime0,$waitingTime0," +
                s"$serviceTimeStart1,$serviceTimeEnd1,$serviceTime1,$waitingTime1,$serviceTimeStart,$serviceTimeEnd,$serviceTime"
            ).pureS
            _                 <- ctx.logger.debug("_____________________________________________").pureS
          } yield ()
        }
        .onError{ e=>
          (ctx.logger.error(e.toString) *> ctx.logger.error(e.getStackTrace.toString)).pureS
        }
      fiberIO          <- request1IO
//        .compile.lastOrError.pureS
//        .start.pureS
//      response1      <- request1IO
    } yield fiberIO
  }
  def processWriteV2(client: Client[IO])(t:Trace)(implicit ctx:AppContextv2) = {
    for {
      _           <- ctx.logger.debug(s"START_UPLOAD ${t.fileId}")
//      waitingTime = t.waitingTime.milliseconds
//      _           <- IO.sleep(waitingTime)
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
                responseNodeId = headers.get(CIString("Node-Id")).map(_.head.value).getOrElse(ctx.config.nodeId)
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
//    _               <- IO.sleep(t.waitingTime milliseconds)
    beforeW         <- IO.monotonic.map(_.toNanos)
    responseHeaders <- client.stream(writeRequest(t)).evalMap{ response =>
      ctx.logger.debug("UPLOAD_RESPONSE_STATUS "+response.status) *> response.headers.pure[IO]
    }.compile
      .last
      .map(_.getOrElse(Headers.empty))
      .onError(e=>ctx.errorLogger.error(e.getMessage))
    responseNodeId = responseHeaders.get(CIString("Node-Id")).map(_.head.value).getOrElse(ctx.config.nodeId)
    responseLevel = responseHeaders.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
    afterW         <- IO.monotonic.map(_.toNanos)
    _              <- ctx.logger.info(s"UPLOAD,$responseNodeId,${t.fileId},${t.fileSize},${afterW-beforeW},0,$responseLevel,${t.operationId}")
  } yield ()

//  def processDownload(t:Trace,client: Client[IO])(implicit ctx:AppContext)= for {
//    _ <- IO.unit
//    _            <- IO.sleep(t.waitingTime milliseconds)
//    beforeW      <- IO.monotonic.map(_.toNanos)
//    downloadIO  =  client.toHttpApp.run(readRequest(t))
//    res <- retryingOnFailures[Response[IO]](
//      wasSuccessful = (a:Response[IO])=>{
//        IO.pure(a.status.code ==200)
//      },
//      onFailure = (a:Response[IO],d:RetryDetails) =>  for {
//        _ <- ctx.logger.debug(s"DOWNLOAD_RESPONSE_STATUS ${t.fileId} ${a.status}")
//        _ <- ctx.errorLogger.error(d.toString)
//      } yield ()
//      ,
//      policy = RetryPolicies.limitRetries[IO](100) join RetryPolicies.exponentialBackoff[IO](1 second)
//    )(downloadIO)
//    body        = res.body
//    writeS      = body.through(Files[IO].writeAll(Paths.get(SINK_PATH.toString,UUID.randomUUID().toString+s".${config.staticExtension}" ))).compile.drain
//    statusPrint = ctx.logger.debug(s"DOWNLOAD_RESPONSE_STATUS ${res.status}")
//    responseHeaders  = res.headers
//    afterW               <- IO.monotonic.map(_.toNanos)
//    responseNodeId       = responseHeaders.get(CIString("Node-Id")).map(_.head.value).getOrElse(config.nodeId)
//    responseLevel        = responseHeaders.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
//    responseTimeDownload = responseHeaders.get(CIString("Response-Time")).map(_.head.value).getOrElse("0")
//    _                    <- ctx.logger.info(s"DOWNLOAD,$responseNodeId,${t.fileId},${t.fileSize},${afterW-beforeW},$responseTimeDownload,$responseLevel,${t.operationId}")
//    _                    <- writeS *> statusPrint
//    //       _ <- for {
//    //    } yield ()
//  } yield ()


  def bytesToString(WORKLOAD_PATH:Path): IO[String] = Files[IO]
    .readAll(WORKLOAD_PATH,8192)
    .compile
    .to(Array)
    .map(new String(_,StandardCharsets.UTF_8))

  def decodeConsumerFile(consumerFileStr:String) = io.circe.parser.decode[Map[String, Int]](consumerFileStr) match {
    case Left(value) => IO.pure(Map.empty[String,Int])
    case Right(value) => value.pure[IO]
  }
  def decodeTraces(traceStr:String)(implicit ctx:AppContextv2): IO[List[Trace]] = io.circe.parser.decode[List[Trace]](traceStr) match {
    case Left(e) => ctx.logger.error(e.getMessage) *> IO.pure(List.empty[Trace])
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
