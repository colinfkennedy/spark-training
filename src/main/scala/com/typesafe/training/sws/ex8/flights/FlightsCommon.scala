package com.typesafe.training.sws.ex8.flights

import com.typesafe.training.util.{CommandLineOptions, FileUtil}
import com.typesafe.training.util.CommandLineOptions.Opt
import com.typesafe.training.data._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.{InputDStream, DStream}
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException
import java.io.{File, PrintStream}

object FlightsCommon {

  /**
   * A function to select the calculation to do, flights between airports
   * or flight delay statistics (default).
   */
  object WhichAnalysis {
    val default = 1

    def apply(value: Option[Int]): Opt = Opt(
      name   = "analysis",
      value  = value.map(_.toString),
      help   = s"-a | --analysis n                 Which analysis to compute: 1 = flight delays (default), 2 = flights between airports over sliding windows.",
      parser = {
        case ("-a" | "--analysis") +: n +: tail => (("analysis", n), tail)
      })
  }

  /**
   * The directory to watch for new files, when using that option.
   */
  object WatchDirectory {
    val default = "output/watch/airline-flights"

    def apply(path: Option[String] = None): Opt = Opt(
      name   = "watch-directory",
      value  = path,
      help   = s"""
         |  -wd | --watch-dir | --watch-directory  dir  The directory to watch for updates.
         |                                             """.stripMargin,
      parser = {
        case ("-wd" | "--watch-dir" | "--watch-directory") +: dir +: tail => (("watch-directory", dir), tail)
      })
  }

  val defaultCheckpointDirectory = "output/streaming-checkpoint"
}

/**
 * Shared code between the different implementations of the Spark Streaming
 * example.
 */
abstract class FlightsCommon {

  val defaultInterval = 2        // 2 second
  val defaultTimeout  = 15       // 15 seconds
  // In production, put this in HDFS, S3, or other resilient filesystem.

  protected var quiet = false

  protected def computeFlightDelays(flights: DStream[Flight]): Unit
  protected def computeFlightsBetweenAirports(
    flights: DStream[Flight], airportsOutPath: String, window: Duration, slide: Duration): Unit

  protected def processOptions(args: Array[String]): Map[String,String] = {
    val options = CommandLineOptions(
      this, "Use --socket or --input-path arguments",
      // For this process, use at least 2 cores!
      CommandLineOptions.master(Some(CommandLineOptions.defaultMaster)),
      FlightsCommon.WatchDirectory(Some(FlightsCommon.WatchDirectory.default)),
      CommandLineOptions.outputPath(Some("output/streaming/airline-stats")),
      CommandLineOptions.socket(None),  // empty default, so we know the user specified this option.
      CommandLineOptions.timeout(Some(defaultTimeout)),
      CommandLineOptions.interval(Some(Seconds(defaultInterval))),
      CommandLineOptions.window(),
      FlightsCommon.WhichAnalysis(Some(FlightsCommon.WhichAnalysis.default)),
      CommandLineOptions.checkpointDirectory(Some(FlightsCommon.defaultCheckpointDirectory)),
      CommandLineOptions.DeleteCheckpointDirectory(Some(true)),
      CommandLineOptions.noterm,
      CommandLineOptions.quiet)
    val argz = options(args.toList)
    argz.getOrElse("analysis", "1") match {
      case "1" | "2" => // okay
      case n =>
        Console.err.println(s"ERROR: Unrecognized number given for the 'analysis' choice: $n")
        sys.exit(1)
    }
    argz
  }

  protected def initMain(args: Array[String]): Unit = {

    val argz = processOptions(args)
    val master = argz("master")
    val quiet = argz.getOrElse("quiet", "false").toBoolean
    val outPathRoot = argz("output-path")
    val interval = argz("interval").toInt
    val (window, slide) = CommandLineOptions.toWindowSlide(argz("window"))
    val airportsOutPath = s"$outPathRoot-airports.csv"
    val checkpointDir = argz("checkpoint-directory")

    if (master.startsWith("local")) {
      val out = argz("output-path")
      if (!quiet) println(s" **** Deleting old output (if any), $out:")
      FileUtil.rmrf(out)
    }
    if (argz("delete-checkpoint-directory").toBoolean) {
      FileUtil.rmrf(checkpointDir)
    }

    // Use a configuration object this time for greater flexibility.
    // "spark.cleaner.ttl" (defaults to infinite) is the duration in seconds
    // of how long Spark will remember any metadata (stages generated,
    // tasks generated, etc.) as well as in-memory RDDs. This is useful for
    // running Spark for a long time, such as 24/7 Streaming apps).
    // See http://spark.apache.org/docs/latest/configuration.html for more details.
    val conf = new SparkConf()
     .setMaster(argz("master"))
     .setAppName("Spark Streaming")
     // .set("spark.cleaner.ttl", "360")         // Defaults to infinity
     .set("spark.files.overwrite", "true")
     .set("spark.executor.memory", "1g")      // When you need more memory:
    val sc  = new SparkContext(conf)

    def createContext(): StreamingContext = {
      val ssc = new StreamingContext(sc, Seconds(interval))
      ssc.checkpoint(checkpointDir)

      val linesDStream = argz.get("socket") match {
        case None => useFiles(ssc, argz("watch-directory"))
        case Some(socket) => useSocket(ssc, socket)
      }
      val flights = for {
        line <- linesDStream
        flight <- Flight.parse(line)
      } yield flight

      if (!quiet) flights.print  // for diagnostics
      argz.getOrElse("analysis", "1") match {
        case "1" => computeFlightDelays(flights)
        case "2" => computeFlightsBetweenAirports(flights, airportsOutPath, window, slide)
        case n =>  // Not okay, but if here, we've already checked that it's 1 or 2!
      }
      ssc
    }

    // Declared as a var so we can see it in the following try/catch/finally blocks.
    var ssc: StreamingContext = null

    try {
      // The preferred way to construct a StreamingContext, because if the
      // program is restarted, e.g., due to a crash, it will pick up where it
      // left off. Otherwise, it starts a new job.
      ssc = StreamingContext.getOrCreate(checkpointDir, createContext _)
      ssc.start()

      val noterm  = argz.getOrElse("no-term", "false").toBoolean
      val timeout = argz("timeout").toInt

      if (noterm) ssc.awaitTermination()
      else ssc.awaitTerminationOrTimeout(timeout * 1000)
    } catch {
      case iie: InvalidInputException =>
        Console.err.println(s"""
          |=====================================================================
          |InvalidInputException thrown:
          |  $iie
          |
          |If the message says that the "input path" doesn't exist and the
          |path includes ${argz("input-path")}, then try deleting the checkpoint
          |directory: $checkpointDir.
          |=====================================================================
          |""".stripMargin)
    } finally {
      if (ssc != null) {
        ssc.stop(stopSparkContext = true, stopGracefully = true)
      }
    }
  }

  private def useSocket(
      sc: StreamingContext, serverPort: String): DStream[String] = {
    try {
      // Pattern match to extract the 0th, 1st array elements after the split.
      val Array(server, port) = serverPort.split(":")
      Console.err.println(s"Connecting to $server:$port...")
      sc.socketTextStream(server, port.toInt).flatMap(_.split("\n"))
    } catch {
      case th: Throwable =>
        sc.stop()
        throw new RuntimeException(
          s"Failed to initialize host:port socket with host:port string '$serverPort':",
          th)
    }
  }

  // Hadoop text file compatible.
  private def useFiles(sc: StreamingContext, pathSpec: String): DStream[String] = {
    Console.err.println(s"Reading 'events' from path $pathSpec")
    sc.textFileStream(pathSpec)
  }

  protected def airportsOut(path: String): PrintStream = {
    val dir = new File(path)
    val parent = dir.getParentFile()
    if (parent.exists == false) parent.mkdirs
    new PrintStream(dir)
  }

  protected def formatRecord(record: ((Int, Int), (Double, Double, Double, Double))): String = record match {
    case ((year, month), (carrierDelay, weatherDelay, nasDelay, lateAircraftDelay)) =>
      "%4d-%02d %10.2f %10.2f %10.2f %10.2f".format(
        year, month, carrierDelay, weatherDelay, nasDelay, lateAircraftDelay)
  }

  protected def printDelaysHeader(id: Int): Unit = {
    println(s"\n======== RDD id: $id ========")
    println(s"Year-Month:     Percent Minutes Delay: (Delay Cause * 100 / Total Elapsed):")
    println(s"            Carrier    Weather       NAS   Late Aircraft")
    println(s"========================================================================")
  }

  protected def printDelaysTrailer(): Unit =
    println(s"========================================================================\n")

}