package mx.cinvestav
import breeze.stats.distributions.{Pareto, RandBasis}
import cats.implicits._
import fs2.io.file.Files
import mx.cinvestav.Delcarations.{DumbObject, baseReadRequest, baseReadRequestV2, readRequestv2}
import org.typelevel.ci.CIString

import java.util.UUID
import scala.util.Random
//
import cats.effect._
import cats.effect.kernel.Resource
//
import fs2.Stream
//
import mx.cinvestav.Delcarations.{AppContextv2, AppStateV2, consumerRequest}
import mx.cinvestav.config.DefaultConfig
//
import org.http4s.HttpRoutes
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.client.Client
import org.http4s.server.Router
import org.http4s.dsl.io._
import org.http4s.implicits._
//
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
//
import java.nio.file.{Path, Paths}
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import language.postfixOps
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
//
import pureconfig._
import pureconfig.generic.auto._

object Main0 extends IOApp{
  implicit val config: DefaultConfig  = ConfigSource.default.loadOrThrow[DefaultConfig]
  val clientResource: Resource[IO, Client[IO]] = BlazeClientBuilder[IO](global)
    .withRequestTimeout(1 minutes)
    .withSocketKeepAlive(true)
    .withSocketReuseAddress(true)
    .withConnectTimeout(1 minutes)
    .resource
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  val unsafeErrorLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLoggerFromName("error")

  final val SINK_PATH                = Paths.get(config.sinkFolder)
  //
  def producer()={
    val BASE_FOLDER_PATH         = Paths.get(config.workloadFolder)
    val BASE_FOLDER_FILE         = BASE_FOLDER_PATH.toFile
    val WRITE_BASE_FOLDER_PATH   = BASE_FOLDER_PATH.resolve("writes")
    val WRITES_JSONs: List[Path] = WRITE_BASE_FOLDER_PATH.toFile.listFiles().toList.map(_.toPath)
    for {
      startTime          <- IO.monotonic.map(_.toSeconds)
      (client,finalizer) <- clientResource.allocated
      initState          = AppStateV2()
      state              <- IO.ref(initState)
      ctx                = AppContextv2(config=config,state=state,logger=unsafeLogger,errorLogger = unsafeErrorLogger,client=client)
      producerIndex      = ctx.config.producerIndex
      producerId         = s"producer-$producerIndex"
      _                  <- ctx.logger.debug(s"PRODUCER_START $producerId")
      jsonStreams        = Stream.emits(WRITES_JSONs).covary[IO].evalMap(Helpers.readJsonStr)
      writes             = jsonStreams.evalMap(Helpers.decodeTraces).map(traces=>Stream.emits(traces).covary[IO])
      consumerUris       = Stream.range(0,ctx.config.consumers).covary[IO].map{ x=>
        val basePort   = ctx.config.consumerPort
        val port       = basePort+x
        val consumerId = if(ctx.config.level == "LOCAL") "localhost" else s"consumer-$x"
        val uri        = if(ctx.config.level == "LOCAL") s"http://$consumerId:$port" else s"http://$consumerId:$basePort"
        uri
      }
      consumersReqs      = (dumbObject:DumbObject) => consumerUris.map(uri=>consumerRequest(uri,dumbObject))
      _                <- writes.parEvalMapUnordered(maxConcurrent = ctx.config.maxConcurrent){ traces=>
        traces.evalMap(t=>
          for {
            _          <- Helpers.processWriteV3(ctx.client)(t)(ctx).compile.drain
            dumbObj    = DumbObject(t.fileId,t.fileSize)
            waitOneSec = IO.sleep(1 second)
            _          <- (waitOneSec *> consumersReqs(dumbObj).evalMap{ x=>
                for {
                  status <- ctx.client.status(x)
                  _      <- ctx.logger.debug(status.toString)
                } yield ()
              }.compile.drain).start
          } yield ()
        ).compile.drain
      }.compile.drain
      endTime         <- IO.monotonic.map(_.toSeconds)
      _               <- ctx.logger.info(s"TOTAL_TIME,0,0,0,0,0,0,${endTime - startTime}")
      _               <- finalizer
    } yield ExitCode.Success
  }

  def consumerHttpApp()(implicit ctx:AppContextv2)= Router[IO](
    "/consume"  -> HttpRoutes.of[IO]{
      case req@POST -> Root / fileId  => for {
        _          <- ctx.logger.debug(s"CONSUME $fileId")
        headers    = req.headers
        objectSize = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
        dumbObj    = DumbObject(fileId,objectSize)
        _          <- ctx.state.update(s=>s.copy(uploadObjects = s.uploadObjects:+ dumbObj ))
        response   <- Ok(s"START_CONSUMING $fileId")
      } yield response
    }
  ).orNotFound

  def  consumeFiles(client: Client[IO])(implicit ctx:AppContextv2) = Stream.constant(0).evalMap{ _ =>
    for {
      currentState    <- ctx.state.get
      uploadedFiles   = currentState.uploadObjects
      numFiles        = ctx.config.numFiles
      currentNumFiles = uploadedFiles.length
      sample = Nil
//      sample          = currentState.pareto.sample(numFiles).take(currentNumFiles)
//        .map(_.ceil.toInt)
//        .sorted
//        .map(x=>if(x >0 && x <10) Random.nextInt(1) else x)
//        .map(x=>if(x>ctx.config.maxDownloads) ctx.config.maxDownloads else x)
      fileIdDownloads = uploadedFiles.zip(sample)
      poolUrl         = ctx.config.poolUrl
      consumerId      = ctx.config.nodeId
      staticExtension = ctx.config.staticExtension
      _ <- Stream.emits(fileIdDownloads).covary[IO].flatMap{
        case (dumbObject,downloadsCounter)=>
          val objectId   = dumbObject.objectId
          val objectSize = dumbObject.size
          Stream.range(start= 0,stopExclusive=downloadsCounter).flatMap{ index =>
              for {
                _               <- Stream.eval(ctx.logger.debug(s"INIT_DOWNLOAD[$index] $objectId $downloadsCounter"))
                startAt         <- Stream.eval(IO.monotonic.map(_.toNanos))
                operationId     = UUID.randomUUID().toString
                request         = readRequestv2(poolUrl =poolUrl, objectId = objectId, objectSize= objectSize, consumerId=consumerId, staticExtension = staticExtension,operationId=operationId)
                response0       <- client.stream(request)
                _               <- Stream.eval(ctx.logger.debug(s"FIRST_REQUEST_SUCCESS ${response0.status}"))
                nodeUrl         <- Stream.eval(response0.as[String])
                _               <- Stream.eval(ctx.logger.debug("NODE_URL "+nodeUrl))
                headers0        = response0.headers
                selectedNodeId  = headers0.get(CIString("Node-Id")).map(_.head.value).getOrElse(ctx.config.nodeId)
                endAt           <- Stream.eval(IO.monotonic.map(_.toNanos))
                serviceTime0    = endAt - startAt
//              REQUEST - 1
                request1        = baseReadRequestV2(nodeUrl)(objectSize=objectSize,consumerId=consumerId,staticExtension=staticExtension,operationId=operationId)
                startAt1        <- Stream.eval(IO.monotonic.map(_.toNanos))
                response1       <- client.stream(request1)
                _               <- Stream.eval(ctx.logger.debug(s"SECOND_REQUEST_SUCCESS ${response1.status}"))
                headers1        = response1.headers
                body            = response1.body
                writeS          = body.through(Files[IO].writeAll(Paths.get(SINK_PATH.toString,s"$operationId.$staticExtension")))
                _               <- if(ctx.config.writeOnDisk) writeS else Stream.eval(IO.unit)
                level           = headers1.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
                endAt1          <- Stream.eval(IO.monotonic.map(_.toNanos))
                serviceTime1    = endAt1 - startAt1
                _               <- Stream.eval(ctx.logger.info(s"DOWNLOAD,$selectedNodeId,$objectId,$objectSize,$serviceTime0,$serviceTime1,$level,$operationId"))
              } yield ()
            }
      }.compile.drain
      _ <- if(fileIdDownloads.nonEmpty) ctx.logger.debug("_______________________________________________") else IO.unit
    } yield ()
  }.interruptAfter(ctx.config.maxDurationMs milliseconds)


  def consumer()= {
    for {
      (client,finalizer) <- clientResource.allocated
      startTime          <- IO.monotonic.map(_.toSeconds)
      initState          = AppStateV2(
//        pareto = Pareto(config.paretoScale,config.paretoScale)(RandBasis.withSeed(config.seed))
      )
      state              <- IO.ref(initState)
      ctx                = AppContextv2(config=config,state=state,logger=unsafeLogger,errorLogger = unsafeErrorLogger,client=client)
      basePort           = ctx.config.consumerPort
      consumerIndex      = ctx.config.consumerIndex
      consumerPort       = basePort+consumerIndex
      _                  <- ctx.logger.debug(s"CONSUMER_START consumer-$consumerIndex on port $consumerPort")
      serverIO           <- BlazeServerBuilder[IO](global)
        .bindHttp(ctx.config.consumerPort,"0.0.0.0")
        .withHttpApp(httpApp = consumerHttpApp()(ctx))
        .serve
        .compile
        .drain.start
      _                  <- consumeFiles(client)(ctx).compile.drain
      _                  <- serverIO.cancel
      endTime            <- IO.monotonic.map(_.toSeconds)
      _                  <- ctx.logger.info(s"TOTAL_TIME,0,0,0,0,0,0,${endTime - startTime}")
      _                  <- finalizer
    } yield ExitCode.Success
  }

  override def run(args: List[String]): IO[ExitCode] = config.role match {
    case "producer"=> producer()
    case "consumer"=> consumer()
    case _=> IO.unit.as(ExitCode.Error)
  }
}
