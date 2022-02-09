package mx.cinvestav

import cats.effect.kernel.Resource
import retry._
import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import fs2.concurrent.Topic
import fs2.io.file.Files
import mx.cinvestav.Delcarations.{AppContext, AppState, Trace, baseReadRequest, baseWriteRequest, readRequest, writeRequest, writeRequestV2}
import mx.cinvestav.Delcarations.Implicits._
import mx.cinvestav.config.DefaultConfig
import org.http4s.Response
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client
import org.typelevel.ci.CIString
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource

import scala.concurrent.ExecutionContext.global
import org.typelevel.log4cats.Logger

import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}
import java.util.UUID
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import concurrent.duration._
import language.postfixOps
import pureconfig._
import pureconfig.generic.auto._

//<- IO.unit

object MainV3 extends IOApp{

  implicit val config: DefaultConfig  = ConfigSource.default.loadOrThrow[DefaultConfig]
  val clientResource: Resource[IO, Client[IO]] = BlazeClientBuilder[IO](global)
    .withRequestTimeout(10 minutes)
    .withMaxTotalConnections(10000)
    .resource
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  val unsafeErrorLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLoggerFromName("error")
  val unsafeQueueLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLoggerFromName("queue")
//
  final val SINK_PATH     = Paths.get(config.sinkFolder)
  final val WORKLOAD_PATH: Path  = Paths.get(config.workloadPath+"/"+config.workloadFilename)


  def daemonPendingDownalods(client: Client[IO])(implicit ctx:AppContext) =
    Stream.awakeEvery[IO](1000 milliseconds).evalMap{ _=>
      for {
        currentState     <- ctx.state.get
        pendingDownloads = currentState.pendingDownloadsV2.values.toList.flatten
        _                <- pendingDownloads.traverse{ t=>
            if(currentState.uploadObjects.contains(t.fileId)) processDownloadV2(t,client).handleError(_=>IO.unit) else IO.unit
        }
        _ <- ctx.logger.debug(s"PENDING_DOWNLOADS ${pendingDownloads.length}")
//        _                <- Logger[IO].debug(pendingDownloads.asJson.toString)
      } yield ()
  }

  def processDownloadV2(t:Trace,client: Client[IO])(implicit ctx:AppContext)= for {
    _               <- IO.sleep(t.waitingTime milliseconds)
    currentState    <- ctx.state.get
    fileId          = t.fileId
    alreadyUploaded = currentState.uploadObjects.contains(fileId)
    _            <- if(alreadyUploaded) {
      for {
        arrivalTimeNanos   <- IO.monotonic.map(_.toNanos)
        request            = readRequest(t)
        responsev2         <- client.toHttpApp.run(request)
        nodeUrl            <- responsev2.as[String]
        serviceTimeNanos0  <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)
//      ___________________________________________________________________________________
        res                <- client.toHttpApp.run(baseReadRequest(nodeUrl,t))
        _ <- if(res.status.code != 200 || responsev2.status.code != 200) {
          for {
            _ <- ctx.logger.debug(s"DOWNLOAD_STATUS ${t.operationId} ${t.fileId} ${res.status.code}")
            _ <- ctx.errorLogger.error(s"DOWNLOAD_STATUS ${t.operationId} ${t.fileId} ${res.status.code}")
          } yield ()
        }
        else {
          for {
            _                  <- ctx.queueLogger.info(s"DEPARTURE ${t.operationId}")
            serviceTimeNanos1  <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)
            body               = res.body
            writeS             = body.through(Files[IO].writeAll(Paths.get(SINK_PATH.toString,s"${t.operationId}.${config.staticExtension}" ))).compile.drain
//            statusPrint        = ctx.logger.debug(s"DOWNLOAD_RESPONSE_STATUS ${res.status}")
            responseHeaders    = res.headers
            responseNodeId     = responseHeaders.get(CIString("Node-Id")).map(_.head.value).getOrElse(config.nodeId)
            responseLevel      = responseHeaders.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
            _                  <- ctx.logger.info(s"DOWNLOAD,$responseNodeId,${t.fileId},${t.fileSize},$serviceTimeNanos0,$serviceTimeNanos1,$responseLevel,${t.operationId}")
            _                  <- ctx.state.update(s=>s.copy(
              pendingDownloadsV2 = s.pendingDownloadsV2.updatedWith(t.fileId)(_.map(_.filter(_.operationId!=t.operationId)).getOrElse(List.empty[Trace]).some ))
            )
            _                  <- writeS
          } yield ()
        }
      } yield ()
    }
    else {
      for {
        _ <- IO.unit
        _ <- ctx.queueLogger.info(s"ENQUEUE ${t.operationId}")
        _ <- ctx.state.update(s=>s.copy(pendingDownloadsV2 = s.pendingDownloadsV2.updatedWith(fileId)( op=> {
            val x = op.map(_ :+t)
            val y = x.getOrElse(List.empty[Trace]).some
            y
          })))
      } yield ()

    }
  } yield ()

  def processWriteV2(t:Trace,client: Client[IO])(implicit ctx:AppContext)= for {
    _              <- IO.sleep(t.waitingTime milliseconds)
    beforeW        <- IO.monotonic.map(_.toNanos)
    response       <- client.toHttpApp.run(writeRequestV2(ctx.config.poolUrl)(t))
    nodeURL        <- response.as[String]
    response       <- client.toHttpApp.run(baseWriteRequest(nodeURL,t,ctx.config.sourceFolder,ctx.config.staticExtension))
    headers        = response.headers
    afterW         <- IO.monotonic.map(_.toNanos)
    responseNodeId = headers.get(CIString("Node-Id")).map(_.head.value).getOrElse(config.nodeId)
    responseLevel  = headers.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
    _              <- ctx.logger.info(s"UPLOAD,$responseNodeId,${t.fileId},${t.fileSize},${afterW-beforeW},0,$responseLevel,${t.operationId}")
    _              <- ctx.state.update(s=>s.copy(uploadObjects = s.uploadObjects :+ t.fileId))
    _              <- ctx.queueLogger.info(s"DEPARTURE ${t.operationId}")
  } yield ()
  def processTrace(t:Trace,client: Client[IO],topic: Topic[IO,String])(implicit ctx:AppContext) = {
    for {
      _            <- ctx.queueLogger.info(s"ARRIVAL ${t.operationId} ${t.operationType}")
      _            <-  if(t.operationType=="W")
        processWriteV2(t,client)(ctx)
          .flatMap(_=>
            topic.publish1(t.fileId)
          )
          .flatMap {
            case Left(value) => ctx.errorLogger.error(s"PUBLISH ${t.operationId}")
            case Right(_) => ctx.logger.debug(s"SUCCESSFULLY_PUBLISH ${t.fileId}")
          }
          .handleError(_=>IO.unit).onError{ e=>
          ctx.logger.debug(s"UPLOAD_ERROR ${e.getMessage}") *> ctx.errorLogger.error(s"${t.operationId} "+e.getMessage)
        }
      else processDownloadV2(t,client)(ctx).handleError(_=>IO.unit)
        .onError{ e=>
        ctx.logger.debug(s"DOWNLOAD_ERROR ${e.getMessage}") *> ctx.errorLogger.error(s"${t.operationId} "+e.getMessage)
      }
      _ <- ctx.logger.debug("_____________________________________________________")
    } yield ()
  }



  override def run(args: List[String]): IO[ExitCode] = {
//    val BASE_FOLDER = Paths.get("/home/nacho/Programming/Scala/experiments-suite/target/workloads/v2/test").toFile
    val BASE_FOLDER = Paths.get(config.workloadFolder).toFile
    val JSONS = BASE_FOLDER.listFiles().toList.map(_.toPath)

    val readJson = (WORKLOAD_PATH:Path) => Files[IO]
      .readAll(WORKLOAD_PATH,8192)
      .compile
      .to(Array)
      .map(new String(_,StandardCharsets.UTF_8))

    val decodeTraces = (traceStr:String ) => io.circe.parser.decode[List[Trace]](traceStr) match {
      case Left(e) =>
        Logger[IO].error(e.getMessage) *> IO.pure(List.empty[Trace])
      case Right(trace) => trace.pure[IO]

    }

    val app = for {
      _             <-  Logger[IO] .debug("SUCCESS")
      topic         <- Topic[IO,String]
//      _ <- topic.pub
      initState     = AppState()
      state         <- IO.ref(initState)
      ctx           = AppContext(config=config,logger = unsafeLogger,state = state,errorLogger = unsafeErrorLogger,queueLogger = unsafeQueueLogger)
      jsonStreams   = Stream.emits(JSONS).covary[IO].evalMap(readJson)
      traces        = jsonStreams.evalMap(decodeTraces).map(traces=>Stream.emits(traces).covary[IO])
      (client,finalizer)    <- clientResource.allocated
//      _ <- daemonPendingDownalods(client)(ctx).compile.drain.start
      _ <- topic.subscribe(maxQueued = 1000).evalMap{ fileId=>
        for {
          currentState <- ctx.state.get
          maybePendingDownloads = currentState.pendingDownloadsV2.get(fileId)
          _ <- maybePendingDownloads match {
            case Some(pendingDownloads) =>
              ctx.logger.debug(s"$fileId has ${pendingDownloads.length} pending downloads") *> Stream
                .emits(pendingDownloads)
                .covary[IO]
                .evalMap(t=>processDownloadV2(t,client)(ctx))
                .compile.drain
            case None => ctx.logger.debug(s"$fileId has no pending downloads")
          }
        } yield ()
      }.compile.drain.start
      _ <- traces.zipWithIndex.parEvalMapUnordered(maxConcurrent = config.maxConcurrent){
        case (sTraces,traceIndex)=> sTraces.zipWithIndex.evalMap{
                case (trace, index) => for {
                  _ <- ctx.logger.debug(s"TRACE-$traceIndex [$index] ${trace.operationId} ${trace.fileId} ${trace.operationType}")
                  _ <- processTrace(trace,client,topic)(ctx)
                } yield ()
              }
          .compile.drain
      }
        .compile
        .drain
      currentState <- ctx.state.get
      _ <- ctx.logger.debug(
        currentState.pendingDownloadsV2.filter(_._2.nonEmpty)
          .toString()
      )
      _ <- ctx.logger.debug("END!")
      _ <- finalizer
    } yield ()




    app.as(ExitCode.Success)
  }
}
