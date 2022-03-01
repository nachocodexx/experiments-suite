package mx.cinvestav

import breeze.stats.distributions.{Pareto, RandBasis}
import cats.data.NonEmptyList
import cats.effect.kernel.Fiber
import cats.effect.{FiberIO, IO, Ref}
import io.circe.{Decoder, HCursor}
import mx.cinvestav.config.DefaultConfig
import org.http4s.client.Client
import org.http4s.{Header, Headers, HttpVersion, MediaType, Method, Request, Uri, headers}
import org.http4s.multipart.{Multipart, Part}
import org.typelevel.ci.CIString
import org.typelevel.log4cats.Logger

import java.io.File
import java.util.UUID

object Delcarations {

  object Implicits {

    implicit val traceDecoder:Decoder[Trace] = (c: HCursor) => for {
      arrivalTime <- c.downField("arrivalTime").as[Long]
//      consumerId <- c.downField("consumerId").as[String]
      fileId <- c.downField("fileId").as[String]
      fileSize <- c.downField("fileSize").as[Long]
      operationId <- c.downField("operationId").as[String]
//      operationType <- c.downField("operationType").as[String]
//      producerId <- c.downField("producerId").as[String]
//      waitingTime <- c.downField("waitingTime").as[Double]
      trace = Trace(
        arrivalTime = arrivalTime,
//        consumerId = consumerId,
        fileId = fileId,
        fileSize = fileSize,
        operationId = operationId,
//        operationType = operationType,
//        producerId = producerId,
//        waitingTime = waitingTime
      )
    } yield trace
  }

  case class Upload(userId:String,operation:String,fileId:String,fileSize:Double,userRole:String)
  case class Download(userId:String,operation:String,fileId:String,fileSize:Double)

  type Traces = List[Trace]
  case class AppState(
                       uploadObjects:List[String]=List(),
                       pendingDownloads:Traces=Nil,
                       pendingDownloadsV2:Map[String,Traces]=Map.empty[String,Traces],
                       lastArrivalTime:Long = 0,
                       fibers:List[FiberIO[Unit]]=Nil
                     )

  case class DumbObject(objectId:String,size:Long)
  case class DownloadTrace(dumbObject: DumbObject,downloads:Int)
//  )
//  implicit val randBasis =
  case class AppStateV2(
                         uploadObjects:List[DumbObject]=Nil,
                         pareto:Pareto= Pareto(1,.95)(RandBasis.withSeed(12345)),
                         fileDownloads:Map[String,Int] = Map.empty[String,Int]
                       )
  case class AppContext(config:DefaultConfig, state:Ref[IO,AppState], logger:Logger[IO],errorLogger:Logger[IO],queueLogger:Logger[IO])
  case class AppContextv2(config:DefaultConfig, state:Ref[IO,AppStateV2], logger:Logger[IO],errorLogger:Logger[IO],client:Client[IO])

  case class Trace(
                    arrivalTime:Long,
//                    consumerId:String,
                    fileId:String,
                    fileSize:Long,
                    operationId:String,
//                    operationType:String,
//                    producerId:String,
//                    waitingTime:Double
                  ) {
    override def toString: String =
      s"$fileId,$fileSize,$operationId"
  }


  def writeRequestV2(baseUrl:String)(trace:Trace)(implicit ctx:AppContextv2):Request[IO] = {
    val req = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(s"$baseUrl/upload"),
      httpVersion = HttpVersion.`HTTP/1.1`
    )
      .putHeaders(
        Headers(
          Header.Raw(CIString("Operation-Id"),trace.operationId),
          Header.Raw(CIString("User-Id"),ctx.config.nodeId),
          Header.Raw(CIString("Bucket-Id"), "nacho-bucket" ),
          Header.Raw(CIString("Object-Id"),trace.fileId),
          Header.Raw(CIString("Object-Size"),trace.fileSize.toString),
          Header.Raw(CIString("Arrival-Time"),trace.arrivalTime.toString)
        )
      )
    req
  }

  def baseWriteRequest(url:String,trace:Trace,sourceFolder:String,staticExtension:String)(implicit ctx:AppContextv2):Request[IO] = {
    val file = new File(s"$sourceFolder/${trace.fileId}.$staticExtension")
//    println(file.toPath)
//    println(s"FILE_EXISTS ${file.exists()}")
    val multipart = Multipart[IO](
      parts = Vector(
        Part.fileData(
          "upload",
          file ,
          headers = Headers(
            Header.Raw(CIString("Object-Id"),trace.fileId),
            headers.`Content-Type`(MediaType.text.plain),
            headers.`Content-Length`(file.length())
          )
        )
      )
    )
    val req = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(url),
      headers = multipart.headers
    )
      .withEntity(multipart)
      .putHeaders(
        Headers(
          Header.Raw(CIString("Operation-Id"),trace.operationId),
          Header.Raw(CIString("User-Id"),ctx.config.nodeId),
          Header.Raw(CIString("Bucket-Id"), "nacho-bucket" ),
          Header.Raw(CIString("Object-Id"),trace.fileId),
//          Header.Raw(CIString("Object-Size"),file.length().toString),
          Header.Raw(CIString("Object-Size"),file.length().toString),
          Header.Raw(CIString("Arrival-Time"),trace.arrivalTime.toString)
        )
      )
    req
  }
  def writeRequest(trace:Trace)(implicit ctx:AppContext):Request[IO] = {
    val file = new File(s"${ctx.config.sourceFolder}/${trace.fileId}.${ctx.config.staticExtension}")
    val multipart = Multipart[IO](
      parts = Vector(
        Part.fileData(
          "upload",
          file ,
          headers = Headers(
            Header.Raw(CIString("Object-Id"),trace.fileId),
            headers.`Content-Type`(MediaType.text.plain),
            headers.`Content-Length`(file.length())
          )
        )
      )
    )
    val req = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(s"${ctx.config.poolUrl}/upload"),
      headers = multipart.headers
    )
      .withEntity(multipart)
      .putHeaders(
        Headers(
          Header.Raw(CIString("Operation-Id"),trace.operationId),
          Header.Raw(CIString("User-Id"),ctx.config.nodeId),
          Header.Raw(CIString("Bucket-Id"), "nacho-bucket" ),
          Header.Raw(CIString("Object-Id"),trace.fileId),
          Header.Raw(CIString("Object-Size"),file.length().toString),
        )
      )
    req
  }

  def baseReadRequestV2(url:String)(objectSize:Long,consumerId:String,staticExtension:String,operationId:String = UUID.randomUUID().toString):Request[IO] ={
    val req = Request[IO](
      method = Method.GET,
      uri = Uri.unsafeFromString(url),
      headers = Headers(
        Header.Raw(CIString("Operation-Id"),operationId),
        Header.Raw(CIString("User-Id"),consumerId),
        Header.Raw(CIString("Bucket-Id"),"default"),
        Header.Raw(CIString("Object-Size"),objectSize.toString),
        Header.Raw(CIString("Object-Extension"),staticExtension),
      )
    )
    req
  }

  def readRequestv2(
                     poolUrl:String,
                     objectId:String,
                     objectSize:Long,
                     consumerId:String,
                     staticExtension:String,
                     operationId:String = UUID.randomUUID().toString,
                   ):Request[IO] ={
    val req = Request[IO](
      method = Method.GET,
      uri = Uri.unsafeFromString( s"$poolUrl/download/$objectId"),
      headers = Headers(
        Header.Raw(CIString("Operation-Id"),operationId),
        Header.Raw(CIString("User-Id"),consumerId),
        Header.Raw(CIString("Bucket-Id"),"default"),
        Header.Raw(CIString("Object-Size"),objectSize.toString),
        Header.Raw(CIString("Object-Extension"),staticExtension),
      )
    )
    req
  }


  def consumerRequest(consumerUri:String,dumbObject: DumbObject)= Request[IO](
    method = Method.POST,
    uri = Uri.unsafeFromString(s"$consumerUri/consume/${dumbObject.objectId}"),
    headers = Headers(Header.Raw(CIString("Object-Size"),dumbObject.size.toString))
  )
  def consumerRequestv2(consumerUri:String, dumbObjects: List[DumbObject])= {
    val objectSizesH = dumbObjects.map{x =>Header.Raw(CIString("Object-Size"),x.size.toString)}
    val objectIdsH   = dumbObjects.map{x =>Header.Raw(CIString("Object-Id"),x.objectId)}
    Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(s"$consumerUri/v2/consume"),

      headers = Headers(objectSizesH) ++ Headers(objectIdsH)
    )
  }

}
