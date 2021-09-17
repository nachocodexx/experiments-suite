name := "experiments-suite"
version := "0.1"
scalaVersion := "2.13.6"
libraryDependencies ++= Dependencies()
assemblyJarName:="experiments-suite.jar"
addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.0" cross CrossVersion.full)
addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
ThisBuild / assemblyMergeStrategy := {
  case x if x.contains("reflect.properties")=> MergeStrategy.last
  case x if x.contains("scala-collection-compat.properties")=> MergeStrategy.last
  case x =>
    //      println(x)
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}
