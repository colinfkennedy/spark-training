import sbt._
import sbt.Keys._

object BuildSettings {

  val Name = "spark-workshop-exercises"
  val Version = "3.0"
  val ScalaVersion = "2.11.7"

  lazy val buildSettings = Defaults.coreDefaultSettings ++ Seq (
    name          := Name,
    version       := Version,
    scalaVersion  := ScalaVersion,
    organization  := "com.typesafe",
    description   := "Spark Workshop",
    scalacOptions := Seq("-deprecation", "-unchecked", "-encoding", "utf8", "-Xlint")
  )
}


object Resolvers {
  val typesafe = "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
  val sonatype = "Sonatype Release" at "https://oss.sonatype.org/content/repositories/releases"
  val mvnrepository = "MVN Repo" at "http://mvnrepository.com/artifact"

  val allResolvers = Seq(typesafe, sonatype, mvnrepository)

}

object Dependency {
  object Version {
    val Spark        = "1.4.1"
    val ScalaTest    = "2.2.4"
    val ScalaCheck   = "1.12.2"
  }

  val sparkCore      = "org.apache.spark"  %% "spark-core"      % Version.Spark  withSources()
  val sparkStreaming = "org.apache.spark"  %% "spark-streaming" % Version.Spark  withSources()
  val sparkSQL       = "org.apache.spark"  %% "spark-sql"       % Version.Spark  withSources()
  val sparkRepl      = "org.apache.spark"  %% "spark-repl"      % Version.Spark  withSources()
  val sparkMLlib     = "org.apache.spark"  %% "spark-mllib"     % Version.Spark  withSources()
  // We're not actually using the Hive module, because it pulls in a LOT of Hadoop dependencies:
  // val sparkHive      = "org.apache.spark"  %% "spark-hive"      % Version.Spark  withSources()

  // For testing.
  val scalaTest      = "org.scalatest"     %% "scalatest"       % Version.ScalaTest  % "test"
  val scalaCheck     = "org.scalacheck"    %% "scalacheck"      % Version.ScalaCheck % "test"

  val playJson       = "com.typesafe.play" %% "play-json"       % "2.3.0"
}

object Dependencies {
  import Dependency._

  val sparkWorkshop =
    Seq(sparkCore, sparkStreaming, sparkSQL, sparkRepl, sparkMLlib,
      scalaTest, scalaCheck, playJson)
}

object SparkWorkshopBuild extends Build {
  import Resolvers._
  import Dependencies._
  import BuildSettings._

  lazy val sparkWorkshop = Project(
    id = "spark-workshop-exercises",
    base = file("."),
    settings = buildSettings ++ Seq(
      maxErrors := 5,
      // Suppress warnings about Scala patch differences in dependencies.
      // This is slightly risky, so consider not doing this for production
      // software, see what the warnings are using the sbt 'evicted' command,
      // then "ask your doctor if this setting is right for you..."
      ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },
      triggeredMessage := Watched.clearWhenTriggered,
      // runScriptSetting,
      resolvers := allResolvers,
      libraryDependencies ++= Dependencies.sparkWorkshop,
      unmanagedResourceDirectories in Compile += baseDirectory.value / "conf",
      mainClass := Some("run"),
      // Must run the examples and tests in separate JVMs to avoid mysterious
      // scala.reflect.internal.MissingRequirementError errors. (TODO)
      fork := true,
      //This is important for some programs to read input from stdin
      connectInput in run := true,
      // Must run Spark tests sequentially because they compete for port 4040!
      // TODO. There is now a Spark property to disable the web console. If we
      // use it, then we can remove the following setting:
      parallelExecution in Test := false))
}



