import sbt._


object Dependencies {
  def apply(): Seq[ModuleID] = {
    lazy val Logback    = "ch.qos.logback" % "logback-classic" % "1.2.3"
    lazy val Fs2IO = "co.fs2" %% "fs2-io" % "3.0.6"
    lazy val Commons = "mx.cinvestav" %% "commons" % "0.0.5"
    lazy val PureConfig = "com.github.pureconfig" %% "pureconfig" % "0.15.0"
    lazy val MUnitCats ="org.typelevel" %% "munit-cats-effect-3" % "1.0.3" % Test
    lazy val Log4Cats =   "org.typelevel" %% "log4cats-slf4j"   % "2.1.1"
    val catsRetryVersion = "3.1.0"
    lazy val CatsRetry = "com.github.cb372" %% "cats-retry" % catsRetryVersion
    val http4sVersion = "1.0.0-M23"
    lazy val Http4s =Seq(
      "org.http4s" %% "http4s-dsl" ,
      "org.http4s" %% "http4s-blaze-server" ,
      "org.http4s" %% "http4s-blaze-client",
      "org.http4s" %% "http4s-circe"
    ).map(_ % http4sVersion)
    val circeVersion = "0.14.1"
    lazy val Circe =  Seq(
      "io.circe" %% "circe-core",
      "io.circe" %% "circe-generic",
      "io.circe" %% "circe-parser"
    ).map(_ % circeVersion)
    Seq(Logback,PureConfig,Commons,MUnitCats,Log4Cats,Fs2IO,CatsRetry) ++ Http4s ++ Circe
  }
}


