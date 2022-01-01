package mx.cinvestav

import retry._
import cats.effect.kernel.Resource
import cats.implicits._
import cats.effect.{ExitCode, IO, IOApp}
import fs2.Stream
import fs2.io.file.Files
import mx.cinvestav.Delcarations.{AppContext, AppState, Trace, baseReadRequest, baseWriteRequest, readRequest, writeRequestV2}
import mx.cinvestav.MainV3.{SINK_PATH, config}
import mx.cinvestav.config.DefaultConfig
import org.http4s.Response
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client
import org.typelevel.ci.CIString
//
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
//
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}
//
import scala.concurrent.ExecutionContext.global
import concurrent.duration._
//
import language.postfixOps
//
import pureconfig._
import pureconfig.generic.auto._
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

object MainV4 extends IOApp{

  implicit val config: DefaultConfig  = ConfigSource.default.loadOrThrow[DefaultConfig]
  val clientResource: Resource[IO, Client[IO]] = BlazeClientBuilder[IO](global)
    .withRequestTimeout(1 minutes)
    .withSocketKeepAlive(true)
    .withMaxWaitQueueLimit(10)
    .withMaxTotalConnections(1000)
    .withSocketReuseAddress(true)
    .withConnectTimeout(1 minutes)
    .resource
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  val unsafeErrorLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLoggerFromName("error")
  val unsafeQueueLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLoggerFromName("queue")

  final val BASE_FOLDER_PATH         = Paths.get(config.workloadFolder)
  final val BASE_FOLDER_FILE         = BASE_FOLDER_PATH.toFile
  final val WRITE_BASE_FOLDER_PATH   = BASE_FOLDER_PATH.resolve("writes")
  final val READS_BASE_FOLDER_PATH   = BASE_FOLDER_PATH.resolve("reads")
  final val readsByFileId            = (fileId:String)=> READS_BASE_FOLDER_PATH.resolve(s"$fileId.json")
  final val WRITES_JSONs: List[Path] = WRITE_BASE_FOLDER_PATH.toFile.listFiles().toList.map(_.toPath)

  def readJsonStr(WORKLOAD_PATH:Path): IO[String] = Files[IO]
    .readAll(WORKLOAD_PATH,8192)
    .compile
    .to(Array)
    .map(new String(_,StandardCharsets.UTF_8))

  def decodeTraces(traceStr:String): IO[List[Trace]] = io.circe.parser.decode[List[Trace]](traceStr) match {
    case Left(e) =>
      Logger[IO].error(e.getMessage) *> IO.pure(List.empty[Trace])
    case Right(trace) => trace.pure[IO]

  }

  def processWrite(client: Client[IO])(t:Trace)(implicit ctx:AppContext) = {
    for {
      _           <- IO.unit
      waitingTime = t.interArrivalTime.milliseconds
//      _           <- ctx.logger.debug(s"WAITING ${t.fileId} $waitingTime")
      _           <- IO.sleep(waitingTime)
      beforeW     <- IO.monotonic.map(_.toNanos)
      response    <- client.stream(writeRequestV2(ctx.config.poolUrl)(t)).evalMap{ response0 =>
           for {
             _ <- IO.unit
             streamReads = for {
               _              <- ctx.state.update(s=>s.copy(uploadObjects = s.uploadObjects :+ t.fileId))
               reads          <- readJsonStr(readsByFileId(t.fileId)).flatMap(decodeTraces)
               _              <- ctx.logger.debug(s"EMITS ${reads.length} downloads")
               fiber          <- Stream.emits(reads).covary[IO].evalMap{
                 read=>retryingOnFailures(
                   policy = RetryPolicies.limitRetries[IO](100) join RetryPolicies.exponentialBackoff[IO](10 milliseconds),
                   wasSuccessful = (result:Boolean)=> result.pure[IO],
                   onFailure = (result:Boolean,details:RetryDetails)=> ctx.errorLogger.error(s"READ ${t.operationId} ${t.fileId}")
                 )(processRead(client)(read))
               }.compile.drain.start
               _              <- ctx.state.update(s=>s.copy(fibers =s.fibers:+ fiber ))
             } yield ()
             headers0          = response0.headers
             isAlreadyUploaded = headers0.get(CIString("Already-Uploaded")).flatMap(x=>x.head.value.toBooleanOption).getOrElse(false)
             _ <- if(isAlreadyUploaded) streamReads
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
                   _              <- streamReads
                 } yield ()
               }.compile.drain
             } yield ()
           }  yield()
      }.compile.drain
    } yield()
  }
  def processRead(client: Client[IO])(t:Trace)(implicit ctx:AppContext):IO[Boolean] = {
    for {
      _                <- IO.unit
      waitingTime      = t.interArrivalTime.milliseconds
      _                <- IO.sleep(waitingTime)
      arrivalTimeNanos <- IO.monotonic.map(_.toNanos)
      request          = readRequest(t)
      //    ______________________________________________________________________
      balancerResponseIO <- client.stream(request).compile.last
      x <- balancerResponseIO match {
        case Some(balancerResponse) => for {
          result                  <- if(balancerResponse.status.code != 200) ctx.errorLogger.error(s"${t.operationId}  ${t.fileId} ${balancerResponse.status} 0") *> IO.pure(false)
          else {
            for {
              nodeUrl            <- balancerResponse.as[String]
              serviceTimeNanos0  <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)
              //      ___________________________________________________________________________________
              res                <- client.stream(baseReadRequest(nodeUrl,t)).evalMap{ res=>
                for {
                  x <- if(res.status.code != 200) ctx.errorLogger.error(s"${t.operationId} ${t.fileId} ${res.status} $nodeUrl") *> IO.pure(false)
                  else {
                    for {
                      serviceTimeNanos1  <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)
                      responseHeaders    = res.headers
                      body               = res.body
                      responseNodeId     = responseHeaders.get(CIString("Node-Id")).map(_.head.value).getOrElse(config.nodeId)
                      responseLevel      = responseHeaders.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
                      _                  <- ctx.logger.info(s"DOWNLOAD,$responseNodeId,${t.fileId},${t.fileSize},$serviceTimeNanos0,$serviceTimeNanos1,$responseLevel,${t.operationId}")
                      writeS             = body.through(Files[IO].writeAll(Paths.get(SINK_PATH.toString,s"${t.operationId}.${config.staticExtension}" ))).compile.drain
                      _ <- if(ctx.config.writeOnDisk) writeS else IO.unit
                    } yield true
                  }
                } yield x
              }.compile.lastOrError.handleError(_=>false)
            } yield res
          }
        } yield result
        case None => IO.pure(false) <* ctx.logger.error(s"NO_RESPONSE ${t.operationId} ${t.fileId}")
      }

    } yield x
  }

  override def run(args: List[String]): IO[ExitCode] = {

    val app = for {
      _                  <- IO.unit
      initState          = AppState()
      state              <- IO.ref(initState)
      ctx                = AppContext(config=config,logger = unsafeLogger,state = state,errorLogger = unsafeErrorLogger,queueLogger = unsafeQueueLogger)
      jsonStreams        = Stream.emits(WRITES_JSONs).covary[IO].evalMap(readJsonStr)
      traces             = jsonStreams.evalMap(decodeTraces).map(traces=>Stream.emits(traces).covary[IO])
      _                  <- ctx.logger.debug(WRITES_JSONs.toString)
      maxConcurrent      = WRITES_JSONs.length
      (client,finalizer) <- clientResource.allocated
      pW                 = (t:Trace) => processWrite(client)(t)(ctx)
      pWv2               = (client:Client[IO],t:Trace) => processWrite(client)(t)(ctx)
//       _ <- traces.evalMap{ ts =>
//           ts.evalMap(t=>pW(t)).compile.drain
//       }.compile.drain
      _ <- traces.parEvalMapUnordered(maxConcurrent = maxConcurrent){ traces =>
        traces.evalMap(pW).compile.drain
//        clientResource.use(c =>  traces.evalMap(pWv2(c,_)).compile.drain)
      }.compile.drain
//      _ <- IO.sleep(10 seconds)
      _ <- ctx.logger.debug("WAITING FOR ALL FIBERS.")
      currentState  <- ctx.state.get
      _ <- currentState.fibers.traverse(f=>f.join)
      _ <- ctx.logger.debug("END")
      _ <- finalizer
    } yield ()
    app.as(ExitCode.Success)
  }
}
