package mx.cinvestav.consumer

import cats.implicits._
import cats.effect._
import mx.cinvestav.helpers.Helpers
//
import fs2.Stream
import fs2.io.file.Files
//
import mx.cinvestav.Delcarations.{AppContextv2, DownloadTrace, DumbObject, baseReadRequestV2, readRequestv2}
import mx.cinvestav.consumer.Consumer.consumeFilesV2
//
import org.http4s.blaze.client.BlazeClientBuilder
import org.typelevel.ci.CIString
//
import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.ExecutionContext.global
//
import scala.concurrent.duration._
import language.postfixOps
//
import mx.cinvestav.commons.Implicits._
import mx.cinvestav.helpers

object Helpers {

  def consumeFilesV3(downloadsTraces:List[List[DownloadTrace]],initTime:Long=0L)(implicit ctx:AppContextv2) = {
    val SINK_PATH       = Paths.get(ctx.config.sinkFolder)
    val poolUrl         = ctx.config.poolUrl
    val consumerId      = ctx.config.nodeId
    val staticExtension = ctx.config.staticExtension
    val maxConcurrent   = ctx.config.maxConcurrent
    val resource        = BlazeClientBuilder[IO](global).resource
    val streamResource = Stream.resource(resource)
    streamResource.flatMap{ client =>
    val ys = downloadsTraces.flatten.flatMap{ dt=>
      List.fill(dt.downloads)(dt.dumbObject)
    }
    scala.util.Random.setSeed(ctx.config.seed)
    val xs = scala.util.Random.shuffle(ys )
      Stream.emits(xs).zipWithIndex.flatMap{
        case (t,index) =>
          for {
            _                 <- IO.sleep(ctx.config.consumerRate milliseconds).pureS
            getNanoTime       = IO.monotonic.map(_.toNanos).map(_ - initTime)
            serviceTimeStart  <- getNanoTime.pureS
//              if(index ==0) 0L.pure[IO].pureS else getNanoTime.pureS
//              IO.monotonic.map(_.toNanos).map(_ - initTime).pureS
            _                 <- ctx.logger.debug(s"DOWNLOAD[$index]").pureS
            objectId          = t.objectId
            objectSize        = t.size
            operationId       = s"operation-$index"
//              UUID.randomUUID().toString
            request           = readRequestv2(
              poolUrl         = poolUrl,
              objectId        = objectId,
              objectSize      = objectSize,
              consumerId      = consumerId,
              staticExtension = staticExtension,
              operationId     = operationId
            )
            response0         <- client.stream(request).onError{ e=>
              ctx.logger.error(e.getMessage).pureS
            }
            _                 <- ctx.logger.debug(s"FIRST_REQUEST_SUCCESS ${response0.status}").pureS
            nodeUrl           <- response0.as[String].pureS.evalMap(x=>ctx.logger.debug(s"SELECTED_NODE_URL $x")*>helpers.Helpers.toURL(x).pure[IO])
            headers0          = response0.headers
            selectedNodeId    = headers0.get(CIString("Node-Id")).map(_.head.value).getOrElse(ctx.config.nodeId)
            waitingTime0      = headers0.get(CIString("Waiting-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
            serviceTime0      = headers0.get(CIString("Service-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
            serviceTimeStart0 = headers0.get(CIString("Service-Time-Start")).flatMap(_.head.value.toLongOption).getOrElse(0L)
            serviceTimeEnd0   = headers0.get(CIString("Service-Time-End")).flatMap(_.head.value.toLongOption).getOrElse(0L)
            endAt             <- getNanoTime.pureS
//              IO.monotonic.map(_.toNanos).pureS
            responseTime0     = endAt - serviceTimeStart
            //          __________________________________________________________
            //          REQUEST - 1
            request1          = baseReadRequestV2(nodeUrl)(objectSize=objectSize,consumerId=consumerId,staticExtension=staticExtension,operationId=operationId)
            startAt1          <- getNanoTime.pureS
//              IO.monotonic.map(_.toNanos).pureS
            fiberIO           <- client.stream(request1)
              .flatMap{ response1 =>
                 for {
                   _ <- IO.unit.pureS
                   _               <- ctx.logger.debug(s"SECOND_REQUEST_SUCCESS ${response1.status}").pureS
                   headers1        = response1.headers
                   waitingTime1    = headers1.get(CIString("Waiting-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                   serviceTime1    = headers1.get(CIString("Service-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                   serviceTimeStart1 = headers1.get(CIString("Service-Time-Start")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                   serviceTimeEnd1   = headers1.get(CIString("Service-Time-End")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                   body            = response1.body
                   writeS          = body.through(Files[IO].writeAll(Paths.get(SINK_PATH.toString,s"$operationId.$staticExtension")))
                   //
                   _               <- if(ctx.config.writeOnDisk) writeS else IO.unit.pureS
                   responseLevel           = headers1.get(CIString("Level")).map(_.head.value).getOrElse("LOCAL")
                   endAt1          <- getNanoTime.pureS
//                     IO.monotonic.map(_.toNanos).pureS
                   responseTime1   = endAt1 - startAt1
                   serviceTimeEnd  <- getNanoTime.pureS
//                     IO.monotonic.map(_.toNanos).map(_ - initTime).pureS
                   serviceTime     = serviceTimeEnd - serviceTimeStart
//                   waitingTime     =  serviceTimeStart - arrivalTime
                   _               <- ctx.logger.info(
                     s"DOWNLOAD,$operationId,$responseLevel,$objectId,$objectSize,$selectedNodeId,$responseTime0,$responseTime1,$serviceTimeStart0,$serviceTimeEnd0,$serviceTime0,$waitingTime0," +
                       s"$serviceTimeStart1,$serviceTimeEnd1,$serviceTime1,$waitingTime1,$serviceTimeStart,$serviceTimeEnd,$serviceTime"
                   ).pureS
                   _               <- ctx.logger.debug("_______________________________________________").pureS
                 } yield ()
              }
              .onError{ e=>ctx.logger.error(e.getMessage).pureS}
              .compile.lastOrError.start.pureS
          } yield fiberIO

      }
    }
  }

  def fromDist(dumbObjects: List[DumbObject],maxConcurrent:Int=2)(implicit ctx:AppContextv2) = {
    for {
      currentState <- ctx.state.get
      maxDownloads = ctx.config.maxDownloads
      numFiles     = dumbObjects.length
      dist         = currentState.pareto
      samples      = (0 until ctx.config.consumerIterations).map(_=>
        dist.sample(numFiles)
          .map(_.ceil.toInt)
          .sorted
          .map(x=>if(x>maxDownloads) maxDownloads else x).toList
      ).toList
      xs           = samples.map{ sample=>
        (dumbObjects zip sample).map {
          case (o,s) => DownloadTrace(o,s)
        }
      }
      _            <- consumeFilesV3(downloadsTraces = xs).compile.drain
    } yield ()
  }
  def fromFileTrace(dumbObjects: List[DumbObject],maxConcurrent:Int=2)(implicit ctx:AppContextv2) = {
    for {
      currentState <- ctx.state.get
      trace        = dumbObjects.map{ o=>
        val downloads = currentState.fileDownloads.getOrElse(o.objectId,0)
        DownloadTrace(o,downloads)
      } :: Nil
      initTime     <- 0L.pure[IO]
//        IO.monotonic.map(_.toNanos)
      _            <- consumeFilesV3(downloadsTraces = trace,initTime=initTime)
        .parEvalMapUnordered(maxConcurrent = ctx.config.maxConcurrent)(x=>x.join)
        .compile
        .drain
    } yield ()
  }

  def base(constantDownloads:Int)(dumbObjects:List[DumbObject],maxConcurrent:Int = 2)(implicit ctx:AppContextv2) = {
    Stream.awakeEvery[IO](period = ctx.config.consumerRate milliseconds).flatMap{ _ =>
      Stream.emits(dumbObjects).parEvalMap[IO,Unit](maxConcurrent = maxConcurrent){ o =>
        val downloadTrace = DownloadTrace(o,downloads = 1)
        val x = consumeFilesV2(downloadsTraces = (downloadTrace::Nil)::Nil)
        x
      }

    }
  }

  def constant(dumbObjects:List[DumbObject],maxConcurrent:Int = 2)(implicit ctx:AppContextv2) =
    base(constantDownloads = 1)(dumbObjects= dumbObjects,maxConcurrent = maxConcurrent)
//  {
//    Stream.awakeEvery[IO](period = ctx.config.consumerRate milliseconds).flatMap{ _ =>
//      Stream.emits(dumbObjects).parEvalMap[IO,Unit](maxConcurrent = maxConcurrent){ o =>
//        val downloadTrace = DownloadTrace(o,downloads = 1)
//        val x = consumeFilesV2(downloadsTraces = (downloadTrace::Nil)::Nil)
//        x
//      }
//
//    }
//  }

}
