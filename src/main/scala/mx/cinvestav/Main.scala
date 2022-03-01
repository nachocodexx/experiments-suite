package mx.cinvestav

import cats.implicits._
import mx.cinvestav.consumer.Consumer
import mx.cinvestav.producer.Producer
import org.typelevel.log4cats.Logger
//
import cats.effect._
//
import mx.cinvestav.config.DefaultConfig
//
import org.typelevel.log4cats.slf4j.Slf4jLogger
//
import scala.language.postfixOps
//
import pureconfig._
import pureconfig.generic.auto._

object Main extends IOApp{
  implicit val config: DefaultConfig  = ConfigSource.default.loadOrThrow[DefaultConfig]
  implicit val unsafeLogger: Logger[IO] = Slf4jLogger.getLogger[IO]
  implicit val unsafeErrorLogger: Logger[IO] = Slf4jLogger.getLoggerFromName("error")

  override def run(args: List[String]): IO[ExitCode] = config.role match {
    case "producer"=>  Producer()(config=config,unsafeLogger = unsafeLogger, unsafeErrorLogger = unsafeErrorLogger)
    case "consumer"=>  Consumer()(config=config,unsafeLogger = unsafeLogger, unsafeErrorLogger = unsafeErrorLogger)
    case _=> IO.unit.as(ExitCode.Error)
  }
}
