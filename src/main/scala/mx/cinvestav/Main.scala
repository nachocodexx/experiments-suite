package mx.cinvestav

import breeze.stats.distributions.{Pareto, RandBasis, ThreadLocalRandomGenerator}
import cats.implicits._
import fs2.io.file.Files
import mx.cinvestav.Delcarations.{DumbObject, baseReadRequestV2, consumerRequestv2, readRequestv2, writeRequestV2}
import org.apache.commons.math3.random.{JDKRandomGenerator, MersenneTwister, SynchronizedRandomGenerator}
import org.typelevel.ci.CIString

import java.util.UUID
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
import org.http4s.dsl.io._
import org.http4s.implicits._
import org.http4s.server.Router
//
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
//
import java.nio.file.{Path, Paths}
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import scala.language.postfixOps
//
import pureconfig._
import pureconfig.generic.auto._

object Main extends IOApp{
  implicit val config: DefaultConfig  = ConfigSource.default.loadOrThrow[DefaultConfig]
  //
  val clientResource: Resource[IO, Client[IO]] = BlazeClientBuilder[IO](global)
    .withRequestTimeout(5 minutes)
    .withSocketKeepAlive(true)
    .withSocketReuseAddress(true)
    .withConnectTimeout(5 minutes)
    .resource
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  val unsafeErrorLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLoggerFromName("error")
  implicit class StreamOpsV[A](io:IO[A]){
    def pureS:Stream[IO,A] = Stream.eval(io)
  }

  final val SINK_PATH                = Paths.get(config.sinkFolder)
  //
  def producer()={
    val BASE_FOLDER_PATH         = Paths.get(config.workloadFolder)
    val BASE_FOLDER_FILE         = BASE_FOLDER_PATH.toFile
    val WRITE_BASE_FOLDER_PATH   = BASE_FOLDER_PATH.resolve("writes")
    val WRITES_JSONs: List[Path] = WRITE_BASE_FOLDER_PATH.toFile.listFiles().toList.map(_.toPath)
    for {

      (client,finalizer) <- clientResource.allocated
      startTime          <- IO.monotonic.map(_.toSeconds)
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
      consumersReqs      = (dumbObject:List[DumbObject]) => consumerUris.map(uri=>consumerRequestv2(uri,dumbObject))
//      consumersReqsv2      = (dumbObject:DumbObject) => consumerUris.map(uri=>consumerRequest(uri,dumbObject))
      ts <- writes.flatMap(identity).zipWithIndex.flatMap{
        case (t,index)=>
        Helpers.processWriteV3(ctx.client)(t,index=index)(ctx).map(_=>t)
//          .zipWithIndex
//          .evalMap{
//            case(r,index)=>
//                for {
//                  _              <- ctx.logger.debug(s"[$index] UPLOAD_END ${t.fileId}")
//                } yield t
//          }
      }.compile.to(List)
//    __________________________________________
      dumbObjs = ts.map(t=>DumbObject(t.fileId,t.fileSize))
      endTime         <- IO.monotonic.map(_.toSeconds)
      _               <- ctx.logger.info(s"TOTAL_TIME,0,0,0,0,0,0,${endTime - startTime}")
      _               <- consumersReqs(dumbObjs).flatMap(ctx.client.stream).compile.drain
//        Stream.emits(ts).map(t=>DumbObject(t.fileId,t.fileSize)).flatMap(dumObj=>).compile.drain
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
    },
    "/v2/consume" -> HttpRoutes.of[IO]{
      case req@POST -> Root => for {
        _           <- ctx.logger.debug(s"START_CONSUMING")
        currentState <- ctx.state.get
        headers     = req.headers
        objectSizes = headers.get(CIString("Object-Size")).map(_.map(_.value)).map(_.map(_.toLong)).get
        objectIds   = headers.get(CIString("Object-Id")).map(_.map(_.value)).get
        zipped      = (objectIds zip objectSizes).map(DumbObject.tupled).toList
        _           <- ctx.state.update(s=>s.copy(uploadObjects = s.uploadObjects.toSet.union(zipped.toSet).toList ))
        maxDownloads = config.maxDownloads
//         numFiles     = config.numFiles
        numFiles     = zipped.length
//        randomGen    = new MersenneTwister(config.seed)
//        randomBas    = new RandBasis(randomGen)
//        dist         = Pareto(scale= config.paretoScale,shape = config.paretoShape)(rand = randomBas)
        dist = currentState.pareto
        samples      = (0 until config.consumerIterations).map(_=>
            dist.sample(numFiles)
              .map(_.ceil.toInt)
              .sorted.map(x=>if(x>maxDownloads) maxDownloads else x).toList
          ).toList
        _           <- consumeFiles(zipped,samples)(ctx).compile.drain.start
        response    <- Ok(s"START_CONSUMING")
      } yield response
    }
  ).orNotFound

  def  consumeFiles(uploadedFiles:List[DumbObject],samples:List[List[Int]])(implicit ctx:AppContextv2): Stream[IO, Unit] = {
//    Stream.range(0,ctx.config.consumerIterations).flatMap{ index =>
    Stream.emits(samples).zipWithIndex.flatMap{
      case (sample,index) =>
      for {
        _               <- ctx.logger.debug(s"ITERATION[$index]").pureS
        currentState    <- ctx.state.get.pureS
//        uploadedFiles   = currentState.uploadObjects
//        numFiles        = ctx.config.numFiles
//        dist            = currentState.pareto
//        sample          =
//          dist.sample(numFiles).map(_.ceil.toInt).sorted.map(x=>if(x>ctx.config.maxDownloads) ctx.config.maxDownloads else x)
        _               <- ctx.logger.debug(s"SAMPLE $sample").pureS
        fileIdDownloads = uploadedFiles.zip(sample)
        poolUrl         = ctx.config.poolUrl
        consumerId      = ctx.config.nodeId
        staticExtension = ctx.config.staticExtension
        maxConcurrent   = ctx.config.maxConcurrent
        client          = ctx.client
        res             <- Stream.emits(fileIdDownloads).covary[IO].parEvalMapUnordered(maxConcurrent = maxConcurrent){
          case (dumbObject,downloadsCounter)=>
            val objectId   = dumbObject.objectId
            val objectSize = dumbObject.size
            Stream.range(start= 0,stopExclusive=downloadsCounter).flatMap{ intraIndex =>
              for {
                _               <- Stream.eval(ctx.logger.debug(s"DOWNLOAD_COUNTER $objectId $index/$intraIndex/$downloadsCounter"))
                startAt         <- Stream.eval(IO.monotonic.map(_.toNanos))
                operationId     = UUID.randomUUID().toString
                request         = readRequestv2(poolUrl =poolUrl, objectId = objectId, objectSize= objectSize, consumerId=consumerId, staticExtension = staticExtension,operationId=operationId)
                response0       <- client.stream(request)
                _               <- Stream.eval(ctx.logger.debug(s"FIRST_REQUEST_SUCCESS ${response0.status}"))
                nodeUrl         <- Stream.eval(response0.as[String])
                _               <- Stream.eval(ctx.logger.debug("NODE_URL "+nodeUrl))
                headers0        = response0.headers
                selectedNodeId  = headers0.get(CIString("Node-Id")).map(_.head.value).getOrElse(ctx.config.nodeId)
                endAt           <- IO.monotonic.map(_.toNanos).pureS
                serviceTime0    = endAt - startAt
                //              REQUEST - 1
                request1        = baseReadRequestV2(nodeUrl)(objectSize=objectSize,consumerId=consumerId,staticExtension=staticExtension,operationId=operationId)
                startAt1        <- IO.monotonic.map(_.toNanos).pureS
                response1       <- client.stream(request1)
                _               <- ctx.logger.debug(s"SECOND_REQUEST_SUCCESS ${response1.status}").pureS
                headers1        = response1.headers
                body            = response1.body
                writeS          = body.through(Files[IO].writeAll(Paths.get(SINK_PATH.toString,s"$operationId.$staticExtension")))
                _               <- if(ctx.config.writeOnDisk) writeS else IO.unit.pureS
                level           = headers1.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
                endAt1          <- IO.monotonic.map(_.toNanos).pureS
                serviceTime1    = endAt1 - startAt1
                _               <- ctx.logger.info(s"DOWNLOAD,$selectedNodeId,$objectId,$objectSize,$serviceTime0,$serviceTime1,$level,$operationId").pureS
                _                <- ctx.logger.debug("_______________________________________________").pureS
                _ <- IO.sleep(1 second).pureS
              } yield ()
            }.compile.drain.onError{ t=>
              ctx.errorLogger.error(t.getMessage)
            }
          //            .start
          //            .start
        }
      } yield ()
    }
  }

  def consumer()= {
    for {
      startTime    <- IO.monotonic.map(_.toSeconds)
      randomGen    = new MersenneTwister(config.seed)
      randomBas    = new RandBasis(randomGen)
      dist         = Pareto(scale= config.paretoScale,shape = config.paretoShape)(rand = randomBas)
      //      dist               = Pareto(scale = config.paretoScale,shape = config.paretoShape)(rand = r
      initState          = AppStateV2(
        pareto = dist
      )
      state              <- IO.ref(initState)
      (client,finalizer) <- clientResource.allocated
      ctx                = AppContextv2(config=config,state=state,logger=unsafeLogger,errorLogger = unsafeErrorLogger,client=client)
      basePort           = ctx.config.consumerPort
      consumerIndex      = ctx.config.consumerIndex
      consumerPort       = if(ctx.config.level=="LOCAL" ) ctx.config.consumerPort + ctx.config.consumerIndex else ctx.config.consumerPort
//        basePort+consumerIndex
      _                  <- ctx.logger.debug(s"CONSUMER_START consumer-$consumerIndex on port $consumerPort")

      serverIO           <- BlazeServerBuilder[IO](global)
        .bindHttp(consumerPort,"0.0.0.0")
        .withHttpApp(httpApp = consumerHttpApp()(ctx))
        .serve
        .compile
        .drain
//      _                  <- serverIO.cancel
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
