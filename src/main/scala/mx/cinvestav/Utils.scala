package mx.cinvestav
import cats.data.EitherT
import cats.implicits._
import cats.effect._

import java.io.FileOutputStream
import java.net.URL
import java.nio.channels.{Channels, ReadableByteChannel}
import scala.util.Try

object Utils {
  def transferE(fos:FileOutputStream,rbc:ReadableByteChannel): EitherT[IO, Throwable, Long] =
    EitherT(
      fos.getChannel.transferFrom(rbc,0,Long.MaxValue)
        .pure[IO]
        .map(_.asRight[Throwable])
    )

  def newChannelE(url: URL): Either[Throwable, ReadableByteChannel] = Either.fromTry(
    Try {
      Channels.newChannel(url.openStream())
    }
  )
    .flatMap(_.asRight[Throwable])

  def downloadFileFormURL(filePath:String,url: URL): EitherT[IO, Throwable, Long] = for {
    rbc           <- EitherT.fromEither[IO](newChannelE(url))
    fos           = new FileOutputStream(filePath)
    transferred   <- transferE(fos,rbc)
  } yield transferred

}
