package mx.cinvestav

import breeze.stats.distributions.{Pareto, RandBasis, ThreadLocalRandomGenerator}
import cats.implicits._
import fs2.io.file.Files
import mx.cinvestav.Delcarations.{DownloadTrace, DumbObject, Trace, baseReadRequestV2, consumerRequestv2, readRequestv2, writeRequestV2}
import org.apache.commons.math3.random.{JDKRandomGenerator, MersenneTwister, SynchronizedRandomGenerator}
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
import mx.cinvestav.commons.Implicits.StreamOps
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

  final val SINK_PATH                = Paths.get(config.sinkFolder)
  final val SOURCE_PATH              = Paths.get(config.sourceFolder)
  //
  def producer()={
    val BASE_FOLDER_PATH         = Paths.get(config.workloadFolder)
    val WRITE_BASE_FOLDER_PATH   = BASE_FOLDER_PATH.resolve("writes")
    val WRITES_JSONs: List[Path] = WRITE_BASE_FOLDER_PATH.toFile.listFiles().toList.map(_.toPath)
    for {
//  _________________________________________________________________________________
      (client,finalizer) <- clientResource.allocated
      startTime          <- IO.monotonic.map(_.toSeconds)
      initState          =  AppStateV2()
      state              <- IO.ref(initState)
      ctx                =  AppContextv2(config=config,state=state,logger=unsafeLogger,errorLogger = unsafeErrorLogger,client=client)
      producerIndex      =  ctx.config.producerIndex
      producerId         =  s"producer-$producerIndex"
      _                  <- ctx.logger.debug(s"PRODUCER_START $producerId")
//  ________________________________________________________________________________
      jsonStreams        = Stream.emits(WRITES_JSONs).covary[IO].evalMap(Helpers.bytesToString)
      writes             = jsonStreams.evalMap(Helpers.decodeTraces).map(traces=>Stream.emits(traces).covary[IO])
      consumerUris       = Stream.range(0,ctx.config.consumers).covary[IO].map{ x=>
        val basePort     = ctx.config.consumerPort
        val port         = basePort+x
        val consumerId   = if(ctx.config.level == "LOCAL") "localhost" else s"consumer-$x"
        val uri          = if(ctx.config.level == "LOCAL") s"http://$consumerId:$port" else s"http://$consumerId:$basePort"
        uri
      }
      consumersReqs      = (dumbObject:List[DumbObject]) => consumerUris.map(uri=>consumerRequestv2(uri,dumbObject))
//  ________________________________________________________________________________
      ts <- if (ctx.config.writeDebug) writes.flatMap(identity).zipWithIndex.evalMap{
        case (t,index) => ctx.logger.debug(s"TRACE[$index] $t")
      }.compile.drain.map(_=>List.empty[Trace])
      else {
        if(ctx.config.producerMode != "CONSTANT") for {
          ts <- writes.flatMap(identity).zipWithIndex.flatMap{
              case (t,index)=>
                Helpers.processWriteV3(ctx.client)(t,index=index)(ctx).map(_=>t)

            }.compile.to(List)
        } yield ts
        else {
          for {
            _             <- IO.unit
            period        = ctx.config.producerRate.milliseconds
            rand          = new Random()
            fileIdAndSize = SOURCE_PATH.toFile.listFiles().toList.map{ p=>
              (p.getName,p.length)
            }
//          ________________________________________________________
            _ <- ctx.logger.debug(fileIdAndSize.toString)
            fileIds   = fileIdAndSize.map(_._1)
            fileSizes = fileIdAndSize.map(_._2)
            _       <- Stream.awakeDelay[IO](period = period).flatMap{ arrivalTime =>
              val consumerId  = s"consumer-0"
              val N           = fileIds.length
              val fileIndex = rand.nextInt(N)
              val fileId      = fileIds(fileIndex)
              val fileSize    = fileSizes(fileIndex)
              val t = Trace(
                arrivalTime   = arrivalTime.toMillis,
                consumerId    = consumerId,
                fileId        = fileId,
                fileSize      = fileSize,
                operationId   = UUID.randomUUID().toString,
                operationType = "W",
                producerId    = ctx.config.nodeId,
                waitingTime   = ctx.config.producerRate
              )
              for{
                _ <- ctx.logger.debug(t.toString).pureS
              } yield ()
//              IO.unit.pureS
            }.compile.drain
            ts = List.empty[Trace]
          } yield ts
        }
      }
//    __________________________________________
      dumbObjs        = ts.map(t=>DumbObject(t.fileId,t.fileSize))
      endTime         <- IO.monotonic.map(_.toSeconds)
      _               <- ctx.logger.debug(s"TOTAL_TIME,0,0,0,0,0,0,${endTime - startTime}")
      _               <- if (ctx.config.readDebug ) IO.unit else consumersReqs(dumbObjs).flatMap(ctx.client.stream).compile.drain
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
        _            <- ctx.logger.debug(s"START_CONSUMING")
        currentState <- ctx.state.get
        headers      = req.headers
        objectSizes  = headers.get(CIString("Object-Size")).map(_.map(_.value)).map(_.map(_.toLong)).get
        objectIds    = headers.get(CIString("Object-Id")).map(_.map(_.value)).get
        _            <- ctx.logger.debug(s"OBJECT_IDS $objectIds")
        dumbObjects       = (objectIds zip objectSizes).map(DumbObject.tupled).toList
        //      _______________________________________________________
        _            <- ctx.state.update(s=>
          s.copy(uploadObjects = s.uploadObjects.toSet.union(dumbObjects.toSet).toList )
        )
        filesDownloads = currentState.fileDownloads
        //      _______________________________________________________
        maxDownloads = config.maxDownloads
        numFiles     = dumbObjects.length
        dist         = currentState.pareto

        samples      = (0 until config.consumerIterations).map(_=>

            dist.sample(numFiles)
              .map(_.ceil.toInt)
              .sorted
              .map(x=>if(x>maxDownloads) maxDownloads else x).toList
          ).toList

        downloadTrace = if (ctx.config.fromConsumerFile) dumbObjects.map{ o=>
            val downloads = currentState.fileDownloads.getOrElse(o.objectId,0)
            DownloadTrace(o,downloads)
          } :: Nil
        else samples.map{ sample=>
          (dumbObjects zip sample).map {
            case (o,s) => DownloadTrace(o,s)
          }
        }

        _           <- ctx.logger.debug(samples.toString)
        _           <- consumeFiles(dumbObjects,samples)(ctx).compile.drain.start
        _           <- ctx.logger.debug("________________________________________________________")

        response    <- Ok(s"START_CONSUMING")
      } yield response
    }
  ).orNotFound

  def consumeFilesV2(downloadsTraces:List[List[DownloadTrace]])(implicit ctx:AppContextv2) = {
    Stream.emits(downloadsTraces).zipWithIndex.flatMap {
      case (dTs,index) => for {
        _               <- ctx.logger.debug(s"ITERATION[$index]").pureS
        currentState    <- ctx.state.get.pureS
        poolUrl         = ctx.config.poolUrl
        consumerId      = ctx.config.nodeId
        staticExtension = ctx.config.staticExtension
        maxConcurrent   = ctx.config.maxConcurrent
        client          = ctx.client
        res             <- Stream.emits(dTs).covary[IO].parEvalMapUnordered(maxConcurrent = maxConcurrent){
          case dt@DownloadTrace(dumbObject,downloads) =>
            val objectId   = dumbObject.objectId
            val objectSize = dumbObject.size
//            val downloadsCounter =
            Stream.range(start= 0,stopExclusive=downloads).flatMap{ intraIndex =>
              for {
                _               <- ctx.logger.debug(s"DOWNLOAD_COUNTER $objectId $index/$intraIndex/$downloads").pureS
                startAt         <- IO.monotonic.map(_.toNanos).pureS
                operationId     = UUID.randomUUID().toString
                request         = readRequestv2(poolUrl =poolUrl, objectId = objectId, objectSize= objectSize, consumerId=consumerId, staticExtension = staticExtension,operationId=operationId)
                response0       <- client.stream(request)
                  .onError{ e=>
                    ctx.logger.error(e.getMessage).pureS
                  }
                _               <- ctx.logger.debug(s"FIRST_REQUEST_SUCCESS ${response0.status}").pureS
                nodeUrl         <- response0.as[String].pureS.evalMap(x=>ctx.logger.debug(s"SELECTED_NODE_URL $x")*>Helpers.toURL(x).pure[IO])
                //                _               <- ctx.logger.debug("NODE_URL "+nodeUrl).pureS
                headers0        = response0.headers
                selectedNodeId  = headers0.get(CIString("Node-Id")).map(_.head.value).getOrElse(ctx.config.nodeId)
                waitingTime0    = headers0.get(CIString("Waiting-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                serviceTime0    = headers0.get(CIString("Service-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                endAt           <- IO.monotonic.map(_.toNanos).pureS
                responseTime0    = endAt - startAt
                //              REQUEST - 1
                request1        = baseReadRequestV2(nodeUrl)(objectSize=objectSize,consumerId=consumerId,staticExtension=staticExtension,operationId=operationId)
                startAt1        <- IO.monotonic.map(_.toNanos).pureS
                response1       <- client.stream(request1).onError{ e=>
                  ctx.logger.error(e.getMessage).pureS
                }

                _               <- ctx.logger.debug(s"SECOND_REQUEST_SUCCESS ${response1.status}").pureS
                //              ?
                headers1        = response1.headers
                waitingTime1    = headers1.get(CIString("Waiting-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                serviceTime1    = headers1.get(CIString("Service-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                body            = response1.body
                writeS          = body.through(Files[IO].writeAll(Paths.get(SINK_PATH.toString,s"$operationId.$staticExtension")))
                //
                _               <- if(ctx.config.writeOnDisk) writeS else IO.unit.pureS
                level           = headers1.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
                endAt1          <- IO.monotonic.map(_.toNanos).pureS
                responseTime1   = endAt1 - startAt1
                _               <- ctx.logger.info(s"DOWNLOAD,$selectedNodeId,$objectId,$objectSize,$responseTime0,$responseTime1,$serviceTime0,$serviceTime1,$waitingTime0,$waitingTime1,$level,$operationId").pureS
                _               <- ctx.logger.debug("_______________________________________________").pureS
                _ <- IO.sleep(1 second).pureS
              } yield ()
            }.compile.drain.onError{ t=>
              ctx.errorLogger.error(t.getMessage)
            }
        }
        _ <- ctx.logger.debug("_________________________________________").pureS
      } yield ()
    }
  }

  def  consumeFiles(uploadedFiles:List[DumbObject],samples:List[List[Int]])(implicit ctx:AppContextv2): Stream[IO, Unit] = {
//    Stream.range(0,ctx.config.consumerIterations).flatMap{ index =>
    Stream.emits(samples).zipWithIndex.flatMap{
      case (sample,index) =>
      for {
        _               <- ctx.logger.debug(s"ITERATION[$index]").pureS
        currentState    <- ctx.state.get.pureS
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
                _               <- ctx.logger.debug(s"DOWNLOAD_COUNTER $objectId $index/$intraIndex/$downloadsCounter").pureS
                startAt         <- IO.monotonic.map(_.toNanos).pureS
                operationId     = UUID.randomUUID().toString
                request         = readRequestv2(poolUrl =poolUrl, objectId = objectId, objectSize= objectSize, consumerId=consumerId, staticExtension = staticExtension,operationId=operationId)
                response0       <- client.stream(request)
                  .onError{ e=>
                  ctx.logger.error(e.getMessage).pureS
                }
                _               <- ctx.logger.debug(s"FIRST_REQUEST_SUCCESS ${response0.status}").pureS
                nodeUrl         <- response0.as[String].pureS.evalMap(x=>ctx.logger.debug(s"SELECTED_NODE_URL $x")*>Helpers.toURL(x).pure[IO])
//                _               <- ctx.logger.debug("NODE_URL "+nodeUrl).pureS
                headers0        = response0.headers
                selectedNodeId  = headers0.get(CIString("Node-Id")).map(_.head.value).getOrElse(ctx.config.nodeId)
                waitingTime0    = headers0.get(CIString("Waiting-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                endAt           <- IO.monotonic.map(_.toNanos).pureS
                serviceTime0    = endAt - startAt
                //              REQUEST - 1
                request1        = baseReadRequestV2(nodeUrl)(objectSize=objectSize,consumerId=consumerId,staticExtension=staticExtension,operationId=operationId)
                startAt1        <- IO.monotonic.map(_.toNanos).pureS
                response1       <- client.stream(request1).onError{ e=>
                  ctx.logger.error(e.getMessage).pureS
                }

                _               <- ctx.logger.debug(s"SECOND_REQUEST_SUCCESS ${response1.status}").pureS
//              ?
                headers1        = response1.headers
                waitingTime1    = headers1.get(CIString("Waiting-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                body            = response1.body
                writeS          = body.through(Files[IO].writeAll(Paths.get(SINK_PATH.toString,s"$operationId.$staticExtension")))
//
                _               <- if(ctx.config.writeOnDisk) writeS else IO.unit.pureS
                level           = headers1.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
                endAt1          <- IO.monotonic.map(_.toNanos).pureS
                serviceTime1    = endAt1 - startAt1
                _               <- ctx.logger.info(s"DOWNLOAD,$selectedNodeId,$objectId,$objectSize,$serviceTime0,$serviceTime1,$waitingTime0,$waitingTime1,$level,$operationId").pureS
                _                <- ctx.logger.debug("_______________________________________________").pureS
                _ <- IO.sleep(1 second).pureS
              } yield ()
            }.compile.drain.onError{ t=>
              ctx.errorLogger.error(t.getMessage)
            }
        }
        _ <- ctx.logger.debug("_________________________________________").pureS
      } yield ()
    }
  }

  def consumer()= {
    val BASE_FOLDER_PATH         = Paths.get(config.workloadFolder)
    val READ_BASE_FOLDER_PATH   = BASE_FOLDER_PATH.resolve("reads")
    for {
      startTime          <- IO.monotonic.map(_.toSeconds)
      randomGen          = new MersenneTwister(config.seed)
      randomBas          = new RandBasis(randomGen)
      dist               = Pareto(scale= config.paretoScale,shape = config.paretoShape)(rand = randomBas)
//
      basePort           = config.consumerPort
      consumerIndex      = config.consumerIndex
      fromConsumerFile   = config.fromConsumerFile
      consumerFileName   = s"${config.nodeId}.json"
      consumerFilePath   = READ_BASE_FOLDER_PATH.resolve(consumerFileName)
      consumerFileStr    <- Helpers.bytesToString(consumerFilePath)
      consumerFileJson   <- Helpers.decodeConsumerFile(consumerFileStr)
//      _                  <- un.debug(consumerFileJson.toString)
      initState          = AppStateV2(
        pareto = dist,
        fileDownloads = consumerFileJson
      )
      state              <- IO.ref(initState)
      (client,finalizer) <- clientResource.allocated
      ctx                = AppContextv2(config=config,state=state,logger=unsafeLogger,errorLogger = unsafeErrorLogger,client=client)
      consumerPort       = if(ctx.config.level=="LOCAL" ) ctx.config.consumerPort + ctx.config.consumerIndex else ctx.config.consumerPort
      _                  <- ctx.logger.debug(s"CONSUMER_START consumer-$consumerIndex on port $consumerPort")
      serverIO           <- BlazeServerBuilder[IO](global)
        .bindHttp(consumerPort,"0.0.0.0")
        .withHttpApp(httpApp = consumerHttpApp()(ctx))
        .serve
        .compile
        .drain
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
