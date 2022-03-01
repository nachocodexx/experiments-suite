package mx.cinvestav.consumer

import breeze.stats.distributions.{Pareto, RandBasis}
import cats.effect._
import cats.effect.kernel.Resource
import cats.implicits._
import mx.cinvestav.helpers.Helpers
import org.typelevel.log4cats.Logger
import shapeless.syntax.std.function.fnUnHListOps
//
import fs2.Stream
import fs2.io.file.Files
//
import mx.cinvestav.Delcarations.{AppContextv2, AppStateV2, DownloadTrace, DumbObject, baseReadRequestV2, readRequestv2}
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.commons.Implicits._
//
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import org.http4s.server.Router
import org.typelevel.ci.CIString
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.client.Client
import org.http4s.implicits._
//
import java.util.UUID
import java.nio.file.Paths
//
import pureconfig._
import pureconfig.generic.auto._
//
import org.apache.commons.math3.random.MersenneTwister
//
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
//
import pureconfig.ConfigSource
//
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import language.postfixOps
import mx.cinvestav.consumer


object Consumer {

  val clientResource: Resource[IO, Client[IO]] = BlazeClientBuilder[IO](global)
    .withRequestTimeout(5 minutes)
    .withConnectTimeout(5 minutes)
    .withMaxWaitQueueLimit(100000)
    .withMaxTotalConnections(100000)
    .resource

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
//        currentState <- ctx.state.get
        headers      = req.headers
        objectSizes  = headers.get(CIString("Object-Size")).map(_.map(_.value)).map(_.map(_.toLong)).get
        objectIds    = headers.get(CIString("Object-Id")).map(_.map(_.value)).get
        _            <- ctx.logger.debug(s"OBJECT_IDS $objectIds")
        dumbObjects   = (objectIds zip objectSizes).map(DumbObject.tupled).toList
        maxConcurrent = ctx.config.maxConcurrent
        //      _______________________________________________________
        _            <- ctx.state.update(s=>
          s.copy(uploadObjects = s.uploadObjects.toSet.union(dumbObjects.toSet).toList )
        )
        _            <- ctx.config.consumerMode match {
          case "CONSTANT"  => consumer.Helpers.constant(dumbObjects=dumbObjects,maxConcurrent=maxConcurrent).compile.drain.start
          case "FROM_FILE" =>
            consumer.Helpers
              .fromFileTrace(
                dumbObjects=dumbObjects,
                maxConcurrent=maxConcurrent
              )
              .start
          case "FROM_DIST" => consumer.Helpers.fromDist(dumbObjects=dumbObjects,maxConcurrent=maxConcurrent).start
        }
        response    <- Ok(s"START_CONSUMING")
        _           <- ctx.logger.debug("________________________________________________________")
      } yield response
    }
  ).orNotFound

  def consumeFilesV2(downloadsTraces:List[List[DownloadTrace]])(implicit ctx:AppContextv2) = {
    val SINK_PATH = Paths.get(ctx.config.sinkFolder)
    BlazeClientBuilder[IO](global).resource.use{ client =>
      Stream.emits(downloadsTraces).zipWithIndex.flatMap {
//  _____________________________________________________________
        case (dTs,index) => for {
          _               <- ctx.logger.debug(s"ITERATION[$index]").pureS
          poolUrl         = ctx.config.poolUrl
          consumerId      = ctx.config.nodeId
          staticExtension = ctx.config.staticExtension
          maxConcurrent   = ctx.config.maxConcurrent
          res             <- Stream.emits(dTs).covary[IO].parEvalMapUnordered(maxConcurrent = maxConcurrent){
            case dt@DownloadTrace(dumbObject,downloads) =>
              val objectId   = dumbObject.objectId
              val objectSize = dumbObject.size
              Stream.range(start= 0,stopExclusive=downloads).flatMap{ intraIndex =>
                for {
                  startAt         <- IO.monotonic.map(_.toNanos).pureS
                  operationId     = UUID.randomUUID().toString
                  request         = readRequestv2(poolUrl =poolUrl, objectId = objectId, objectSize= objectSize, consumerId=consumerId, staticExtension = staticExtension,operationId=operationId)
                  response0       <- client.stream(request).onError{ e=>
                    ctx.logger.error(e.getMessage).pureS
                  }
                  _               <- ctx.logger.debug(s"FIRST_REQUEST_SUCCESS ${response0.status}").pureS
                  nodeUrl         <- response0.as[String].pureS.evalMap(x=>ctx.logger.debug(s"SELECTED_NODE_URL $x")*>Helpers.toURL(x).pure[IO])
                  headers0        = response0.headers
                  selectedNodeId  = headers0.get(CIString("Node-Id")).map(_.head.value).getOrElse(ctx.config.nodeId)
                  waitingTime0    = headers0.get(CIString("Waiting-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                  serviceTime0    = headers0.get(CIString("Service-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                  endAt           <- IO.monotonic.map(_.toNanos).pureS
                  responseTime0    = endAt - startAt
                  //              REQUEST - 1
                  request1        = baseReadRequestV2(nodeUrl)(objectSize=objectSize,consumerId=consumerId,staticExtension=staticExtension,operationId=operationId)
                  startAt1        <- IO.monotonic.map(_.toNanos).pureS
                  response1       <- client.stream(request1).onError{ e=>ctx.logger.error(e.getMessage).pureS}
                  _               <- ctx.logger.debug(s"SECOND_REQUEST_SUCCESS ${response1.status}").pureS
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
                } yield ()
              }.compile.drain.onError{ e=>ctx.errorLogger.error(e.getMessage)}
          }
        } yield ()
      }.compile.drain
//  ________________________________________________________________
    }
//
  }

  def apply()(implicit config:DefaultConfig,unsafeLogger:Logger[IO],unsafeErrorLogger:Logger[IO]) = {
//    val SOURCE_PATH              = Paths.get(config.sourceFolder)
    val BASE_FOLDER_PATH         = Paths.get(config.workloadFolder)
    val READ_BASE_FOLDER_PATH   = BASE_FOLDER_PATH.resolve("reads")
    for {
      startTime          <- IO.monotonic.map(_.toSeconds)
      randomGen          = new MersenneTwister(config.seed)
      randomBas          = new RandBasis(randomGen)
      dist               = Pareto(scale= config.paretoScale,shape = config.paretoShape)(rand = randomBas)
      //
//      basePort           = config.consumerPort
      consumerIndex      = config.consumerIndex
//      fromConsumerFile   = config.fromConsumerFile
      fileDownloads      <- if(config.consumerMode == "FROM_FILE") for {
        _ <- IO.unit
        consumerFileName   = s"${config.nodeId}.json"
        consumerFilePath   = READ_BASE_FOLDER_PATH.resolve(consumerFileName)
        consumerFileStr    <- Helpers.bytesToString(consumerFilePath)
        consumerFileJson   <- Helpers.decodeConsumerFile(consumerFileStr)
      } yield consumerFileJson
      else Map.empty[String,Int].pure[IO]
      //      _                  <- un.debug(consumerFileJson.toString)
      initState          = AppStateV2(
        pareto = dist,
        fileDownloads = fileDownloads
      )
      state              <- IO.ref(initState)
      (client,finalizer) <- clientResource.allocated
      ctx                = AppContextv2(config=config,state=state,logger=unsafeLogger,errorLogger = unsafeErrorLogger,client=client)
      consumerPort       = if(ctx.config.appMode=="LOCAL" ) ctx.config.consumerPort + ctx.config.consumerIndex else ctx.config.consumerPort
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

}
