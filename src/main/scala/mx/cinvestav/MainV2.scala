package mx.cinvestav

import cats.implicits._
import cats.effect.kernel.Resource
import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import fs2.io.file.Files
import io.circe.Decoder.Result
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.Delcarations.{AppContext, AppState, Trace, readRequest, writeRequest}
import mx.cinvestav.config.DefaultConfig
import org.http4s.Headers
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client
import org.typelevel.ci.CIString
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource
import mx.cinvestav.Delcarations.Implicits._
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}
import scala.concurrent.ExecutionContext.global
import concurrent.duration._
import language.postfixOps
import pureconfig._
import pureconfig.generic.auto._

import java.util.UUID

object MainV2 extends IOApp{
  implicit val config: DefaultConfig  = ConfigSource.default.loadOrThrow[DefaultConfig]
  val clientResource: Resource[IO, Client[IO]] = BlazeClientBuilder[IO](global)
    .withRequestTimeout(10 minutes)
    .resource

  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  val unsafeErrorLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLoggerFromName("error")
  final val SINK_PATH     = Paths.get(config.sinkFolder)
  final val WORKLOAD_PATH: Path  = Paths.get(config.workloadPath+"/"+config.workloadFilename)
//  final val TRACE_PATH = "/home/nacho/Programming/Scala/experiments-suite/target/workloads/TRACE_10.json"




  def processPendingDownloads(client: Client[IO])(implicit ctx:AppContext) = Stream.awakeEvery[IO](10 seconds).evalMap{ t=> for {
    currentState     <- ctx.state.get
    pendingDownloads = currentState.pendingDownloads
    _                <- ctx.logger.debug(s"PENDING_DOWNLOADS ${pendingDownloads.length}")
    _                <- pendingDownloads.traverse{ t=> processDownload(t,client)}
  } yield ()
  }

  def processWrite(t:Trace,client: Client[IO])(implicit ctx:AppContext)= for {
    currentState    <- ctx.state.get
    waitingTime     = t.arrivalTime - currentState.lastArrivalTime
//    _               <- Logger[IO].debug(s"UPLOAD ${t.fileId} $waitingTime")
    _               <- Logger[IO].debug(s"WAITING_TIME $waitingTime")
    _               <- IO.sleep(waitingTime milliseconds)
    beforeW         <- IO.monotonic.map(_.toNanos)
    responseHeaders <- client.stream(writeRequest(t)).evalMap{ response =>
      Logger[IO].debug("UPLOAD_RESPONSE_STATUS "+response.status) *> response.headers.pure[IO]
    }.compile
      .last
      .map(_.getOrElse(Headers.empty))
      .onError(e=>ctx.errorLogger.error(e.getMessage))
    responseNodeId = responseHeaders.get(CIString("Node-Id")).map(_.head.value).getOrElse(config.nodeId)
    responseLevel = responseHeaders.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
    afterW         <- IO.monotonic.map(_.toNanos)
    _              <- Logger[IO].info(s"UPLOAD,$responseNodeId,${t.fileId},${t.fileSize},${afterW-beforeW},0,$responseLevel,${t.operationId}")
    _               <- ctx.state.update(s=>s.copy(
      lastArrivalTime = t.arrivalTime.toLong,
      uploadObjects = s.uploadObjects :+ t.fileId
    ))
  } yield ()
  def processDownload(t:Trace,client: Client[IO])(implicit ctx:AppContext)= for {
    currentState <- ctx.state.get
    isAlreadyUploaded = currentState.uploadObjects.contains(t.fileId)
    _            <- if(!isAlreadyUploaded) for {
      _ <- if(currentState.pendingDownloads.map(_.operationId).contains(t.operationId)) IO.unit
      else ctx.state.update{ s=>s.copy(pendingDownloads = s.pendingDownloads:+t)}
    }  yield ()
    else {
       for {
         _ <- IO.unit
         waitingTime  = t.arrivalTime - currentState.lastArrivalTime
         _            <- Logger[IO].debug(s"WAITING_TIME $waitingTime")
         _            <- IO.sleep(waitingTime milliseconds)
         _            <- ctx.state.update(_.copy(lastArrivalTime = t.arrivalTime.toLong))
         beforeW      <- IO.monotonic.map(_.toNanos)
         _  <- client.stream(readRequest(t)).evalMap{ res=>
           val body        = res.body
           val writeS      = body.through(Files[IO].writeAll(Paths.get(SINK_PATH.toString,UUID.randomUUID().toString+s".${config.staticExtension}" ))).compile.drain
           val statusPrint = Logger[IO].debug(s"DOWNLOAD_RESPONSE_STATUS ${res.status}")
           val responseHeaders  = res.headers
           for {
             afterW               <- IO.monotonic.map(_.toNanos)
             responseNodeId       = responseHeaders.get(CIString("Node-Id")).map(_.head.value).getOrElse(config.nodeId)
             responseLevel        = responseHeaders.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
             responseTimeDownload = responseHeaders.get(CIString("Response-Time")).map(_.head.value).getOrElse("0")
             _                    <- Logger[IO].info(s"DOWNLOAD,$responseNodeId,${t.fileId},${t.fileSize},${afterW-beforeW},$responseTimeDownload,$responseLevel,${t.operationId}")
             _                    <- writeS *> statusPrint
           } yield ()
         }
           .compile
           .drain
           .onError(e=>ctx.errorLogger.error(e.getMessage))
       } yield ()
    }
  } yield ()

  override def run(args: List[String]): IO[ExitCode] = {
    val eventsString = Files[IO]
      .readAll(WORKLOAD_PATH,8192)
      .compile
      .to(Array)
      .map(new String(_,StandardCharsets.UTF_8))

    eventsString
      .flatMap{ traceString=>
        io.circe.parser.decode[List[Trace]](traceString) match {
          case Left(e) =>
            Logger[IO].error(e.getMessage)
          case Right(trace) => for {
            _         <- IO.unit
            initState = AppState()
            state     <- IO.ref(initState)
            ctx       = AppContext(
              config=config,
              logger = unsafeLogger,
              state = state,
              errorLogger = unsafeErrorLogger,
              queueLogger = unsafeErrorLogger
            )
            (client,finalizer)    <- clientResource.allocated
            _         <- Stream.emits(trace).evalMap{ t =>
              if(t.operationType=="W") processWrite(t,client)(ctx) else processDownload(t,client)(ctx)
            }
              .concurrently(processPendingDownloads(client)(ctx=ctx))
              .compile.drain
            _ <- finalizer
          } yield ()

        }
      }
      .as(ExitCode.Success)
//    IO.unit.as(ExitCode.Success)
  }
}
