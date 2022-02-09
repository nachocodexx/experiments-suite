import cats.implicits._
import cats.effect._
import fs2.io.file.Files
import io.circe.{Decoder, HCursor, Json}
import io.circe.parser.{decode, parse}
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.commons.events.{AddedNode, Downloaded, EventX, Evicted, Get, Missed, Put, Uploaded}

import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.Paths

class ExperimentSpec extends munit .CatsEffectSuite {

  test("Net"){
    val _uri ="http://127.0.0.1:6666/api/v2"
    val uri  = new URL(_uri)
    val u    = new URL(uri.getProtocol,"my-node",uri.getPort,uri.getPath)
    println(u)
//    uri.
  }
//  val basevent = Put(
//        eventId = "",
//        serialNumber = 0,
//        nodeId = "",
//        objectId = "",
//        objectSize = 0,
//        timestamp = 0,
//        eventType = "PUT",
//        serviceTimeNanos = 0
//      )
  implicit val eventDecoder:Decoder[EventX] = (hCursor:HCursor) =>{
    for {
      eventType <- hCursor.get[String]("eventType")
      decoded   <- eventType match {
        case "EVICTED" => hCursor.as[Evicted]
        case "UPLOADED" => hCursor.as[Uploaded]
        case "DOWNLOADED" => hCursor.as[Downloaded]
        case "PUT" => hCursor.as[Put]
        case "GET" => hCursor.as[Get]
        case "MISSED" => hCursor.as[Missed]
        case "ADDED_NODE" => hCursor.as[AddedNode]
      }
    } yield decoded
  }

  test("Basic"){
    val events = Files[IO]
      .readAll(Paths.get("/home/nacho/Programming/Scala/experiments-suite/target/source/LFU_5-10_ex0" +
        ".json"),8192)
      .compile
      .to(Array)
      .map(new String(_,StandardCharsets.UTF_8))

    events
      .flatMap{ inputString=>
        decode[List[EventX]](inputString) match {
          case Left(value) =>
            IO.delay{List.empty[EventX]} *> IO.println(s"ERROR: $value")
          case Right(value) =>
            IO.pure(value)
        }
      }
    .flatMap(IO.println)

  }

}
