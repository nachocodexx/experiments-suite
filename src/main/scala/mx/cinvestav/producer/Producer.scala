package mx.cinvestav.producer

import cats.effect.kernel.Resource
import cats.effect.{ExitCode, IO}
import fs2.Stream
import mx.cinvestav.Delcarations.{AppContextv2, AppStateV2, DumbObject, Trace, consumerRequestv2}
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.commons.Implicits._
import mx.cinvestav.helpers.Helpers
import org.typelevel.log4cats.Logger
//

import scala.concurrent.ExecutionContext.global
//import mx.cinvestav.Main.{SOURCE_PATH, clientResource, config, unsafeErrorLogger, unsafeLogger}

import java.nio.file.{Path, Paths}
import java.util.UUID
//
import scala.util.Random
import scala.concurrent.duration._
import language.postfixOps
//
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client
//
import cats.implicits._
import cats.effect._

object Producer {
  //

  def apply()(implicit config:DefaultConfig,unsafeLogger:Logger[IO],unsafeErrorLogger:Logger[IO]) = {
    val SINK_PATH                = Paths.get(config.sinkFolder)
    val SOURCE_PATH              = Paths.get(config.sourceFolder)
    val BASE_FOLDER_PATH         = Paths.get(config.workloadFolder)
    val WRITE_BASE_FOLDER_PATH   = BASE_FOLDER_PATH.resolve("writes")
//    val WRITES_JSONs: List[Path] = WRITE_BASE_FOLDER_PATH.toFile.listFiles().toList.map(_.toPath)
    val clientResource: Resource[IO, Client[IO]] = BlazeClientBuilder[IO](global)
      .withRequestTimeout(5 minutes)
      .withConnectTimeout(5 minutes)
      .withMaxTotalConnections(100000)
      .withMaxWaitQueueLimit(1024)
      .resource
    for {
      //  _________________________________________________________________________________
      (client,finalizer) <- clientResource.allocated
      startTime          <- IO.monotonic.map(_.toSeconds)
      initState          =  AppStateV2()
      state              <- IO.ref(initState)
      ctx                =  AppContextv2(config=config,state=state,logger=unsafeLogger,errorLogger = unsafeErrorLogger,client=client)
      implicit0(_ctx:AppContextv2) <- ctx.pure[IO]
      producerIndex      =  ctx.config.producerIndex
      producerId         =  s"producer-$producerIndex"
      _                  <- ctx.logger.debug(s"PRODUCER_START $producerId")
      //  ________________________________________________________________________________
      producerTracePath  = WRITE_BASE_FOLDER_PATH
        .resolve(s"${ctx.config.nodeId}.json")
      jsonStreams        = Stream.emit(producerTracePath).evalMap(Helpers.bytesToString)
//        Stream.emits(WRITES_JSONs).covary[IO].evalMap(Helpers.bytesToString)
      writes             = jsonStreams.evalMap(Helpers.decodeTraces).map(traces=>Stream.emits(traces).covary[IO])
      consumerUris       = Stream.range(0,ctx.config.consumers).covary[IO].map{ x=>
        val basePort     = ctx.config.consumerPort
        val port         = basePort+x
        val consumerId   = if(ctx.config.appMode == "LOCAL") "localhost" else s"consumer-$x"
        val uri          = if(ctx.config.appMode == "LOCAL") s"http://$consumerId:$port" else s"http://$consumerId:$basePort"
        uri
      }
      consumersReqs      = (dumbObject:List[DumbObject]) => consumerUris.map(uri=>consumerRequestv2(uri,dumbObject))
      //  ________________________________________________________________________________
      initTime           <- 0.pure[IO]
//        IO.monotonic.map(_.toMillis)
      ts <- if (ctx.config.writeDebug) writes.flatMap(identity).zipWithIndex.evalMap{
        case (t,index) => ctx.logger.debug(s"TRACE[$index] $t")
      }.compile.drain.map(_=>List.empty[Trace])
      else {
//        val c = BlazeClientBuilder[IO](global)
        ctx.config.producerMode match {
          case "FROM_FILE" => writes.flatMap(identity).zipWithIndex.parEvalMapUnordered(maxConcurrent = ctx.config.maxConcurrent){
              case (t,index)=>
                val x = Helpers.processWriteV3(ctx.client)(t,initTime = initTime,index=index)(ctx).compile.lastOrError
//                val y = x.flatMap(_.join)
                x.map(_=> t)
            }.compile.to(List)
          case "CONSTANT" => for {
              _             <- IO.unit
              period        = ctx.config.producerRate.milliseconds
              rand          = new Random()
              fileIdAndSize = SOURCE_PATH.toFile.listFiles().toList.map{ p=>
                val nameSplitted = p.getName.split('.')
                val maybeName    = nameSplitted.headOption
                maybeName match {
                  case Some(value) =>(value,p.length)
                  case None => (p.getName,p.length)
                }
              }
              //          ________________________________________________________
              _ <- ctx.logger.debug(fileIdAndSize.toString)
              fileIds   = fileIdAndSize.map(_._1)
              fileSizes = fileIdAndSize.map(_._2)
//              initTime  <- IO.monotonic.map(_.toMillis)
              _       <- Stream.awakeDelay[IO](period = period).zipWithIndex.flatMap{
                case (arrivalTime,index) =>
                  val N           = fileIds.length
                  val fileIndex   = rand.nextInt(N)
//                  val consumerId  = s"consumer-0"
                  val fileId      = fileIds(fileIndex)
                  val fileSize    = fileSizes(fileIndex)
                  val t           = Trace(
                    arrivalTime   = arrivalTime.toMillis,
                    fileId        = fileId,
                    fileSize      = fileSize,
                    operationId   = UUID.randomUUID().toString,
                  )
                  for{
                    currentState <- ctx.state.get.pureS
                    uploads      = currentState.uploadObjects
                    objectIds    = uploads.map(_.objectId)
                    _            <- if(objectIds.contains(t.fileId)) IO.unit.pureS else {
                      for {
                        _            <- ctx.logger.debug(t.toString).pureS
                        _            <- Helpers.processWriteV3(ctx.client)(t,initTime = initTime,index=index)(ctx)
                        reqs         <- consumersReqs(List(DumbObject(t.fileId,t.fileSize))).flatMap(ctx.client.stream)
                        _            <- ctx.logger.debug(s"RESPONSE $reqs").pureS
                        o            = DumbObject(t.fileId,t.fileSize)
                        _            <- ctx.state.update(s=>s.copy(uploadObjects = s.uploadObjects :+ o)).pureS
                      } yield ()
                    }
                  } yield ()
              }.compile.drain
              ts = List.empty[Trace]
            } yield ts
        }
      }
      dumbObjs        = ts.map(t=>DumbObject(t.fileId,t.fileSize))
      endTime         <- IO.monotonic.map(_.toSeconds)
      _               <- ctx.logger.debug(s"TOTAL_TIME,0,0,0,0,0,0,${endTime - startTime}")
      _               <- if (ctx.config.readDebug && ctx.config.producerMode != "CONSTANT" )
        IO.unit
      else consumersReqs(dumbObjs).flatMap(ctx.client.stream).compile.drain
      _               <- finalizer
    } yield ExitCode.Success
  }

}
