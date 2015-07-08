package com.typesafe.training.sws.ex8

import com.typesafe.training.util.{CommandLineOptions, FileUtil}
import com.typesafe.training.util.CommandLineOptions.Opt
import com.typesafe.training.util.streaming._
import org.apache.spark.streaming.Seconds
import scala.sys.process._
import scala.language.postfixOps
import java.net.URL
import java.io.File

/**
 * The driver program for the SparkStreaming example and variations.
 * It handles management of separate threads to provide the source data,
 * either over a socket or by periodically writing new data files to a
 * directory. It also has a "--sql" option to run the SparkStreamingSQL
 * variant.
 * Run with the --help option for details.
 */
object SparkStreamingMain {

  val defaultInterval = 2        // 2 second
  val defaultTimeout  = 15       // 15 seconds

  /**
   * Run the SQL example instead.
   */
  def useSQL: Opt = Opt(
    name   = "sql",
    value  = None,
    help   = s"--sql                             Invoke the SQL version of the example",
    parser = {
      case "--sql" +: tail => (("sql", "true"), tail)
    })

  /**
   * The source data file to write over a socket or to write repeatedly to the
   * directory specified by --inpath.
   */
  def sourceDataFile(path: Option[String] = None): Opt = Opt(
    name   = "source-data-file",
    value  = path,
    help   = s"""
       |  -d | --data  file                 The source data file used for the data.
       |                                   """.stripMargin,
    parser = {
      case ("-d" | "--data") +: file +: tail => (("source-data-file", file), tail)
    })

  /**
   * By default, we want to delete the watched directory and its contents for
   * the exercise, but if you specify an "--inpath directory" that you DON'T
   * want deleted, then pass "--remove-watched-dir false".
   */
  def removeWatchedDirectory(trueFalse: Option[Boolean] = Some(true)): Opt = Opt(
    name   = "remove-watched-dir",
    value  = trueFalse.map(_.toString),
    help   = s"--remove-watched-dir true|false   Remove the 'watched' directory (the --inpath path)?",
    parser = {
      case "--remove-watched-dir" +: tf +: tail => (("remove-watched-dir", tf), tail)
    })

  def main(args: Array[String]): Unit = {
    val options = CommandLineOptions(
      this.getClass.getSimpleName, "Use --socket or --input-path arguments",
      // For this process, use at least 2 cores!
      CommandLineOptions.master(Some("local[*]")),
      CommandLineOptions.inputPath(Some("data/airline-flights/tmp")),
      CommandLineOptions.outputPath(Some("output/streaming/airline-stats/data.csv")),
      CommandLineOptions.socket(None),  // empty default, so we know the user specified this option.
      CommandLineOptions.interval(Some(Seconds(defaultInterval))),
      CommandLineOptions.window(),
      useSQL,
      sourceDataFile(Some("data/airline-flights/alaska-airlines/2008.csv")),
      removeWatchedDirectory(Some(true)),
      SparkStreamingCommon.WhichAnalysis(Some(1)),
      CommandLineOptions.noterm,
      CommandLineOptions.quiet)
    val argz   = options(args.toList)

    val master = argz("master")
    val sql    = argz.getOrElse("sql", "false").toBoolean
    val quiet  = argz.getOrElse("quiet", "false").toBoolean
    val in     = argz("input-path")
    val out    = argz("output-path")
    val data   = argz("source-data-file")
    val socket = argz.getOrElse("socket", "")
    val rmWatchedDir = argz("remove-watched-dir").toBoolean

    // Need to remove some arguments before calling SparkStreaming*.
    def mkStreamArgs(argsSeq: Seq[String], newSeq: Vector[String]): Vector[String] =
      argsSeq match {
        case Nil => newSeq
        case ("-d" | "--data") +: file +: tail => mkStreamArgs(tail, newSeq)
        case "--remove-watched-dir" +: tf +: tail => mkStreamArgs(tail, newSeq)
        case "--sql" +: tail => mkStreamArgs(tail, newSeq)
        case head +: tail => mkStreamArgs(tail, newSeq :+ head)
      }
    val streamArgs = mkStreamArgs(args, Vector.empty[String]).toArray

    if (master.startsWith("local")) {
      if (!quiet) println(s" **** Deleting old output (if any), $out:")
      FileUtil.rmrf(out)
    }

    try {
      val dataThread =
        if (socket == "") {
          startDirectoryDataThread(in, data)
        } else {
          val port = socket.split(":").last.toInt
          startSocketDataThread(port, data)
        }
      if (sql) SparkStreamingSQL.main(streamArgs)
      else     SparkStreaming.main(streamArgs)
      // When SparkStreaming*.main returns, we can terminate the data server thread:
      dataThread.interrupt()
    } finally {
      if (rmWatchedDir) FileUtil.rmrf(in)
    }
  }

  def startSocketDataThread(port: Int, dataFile: String): Thread = {
    val dataThread = new Thread(new DataSocketServer(port, dataFile))
    dataThread.start()
    dataThread
  }
  def startDirectoryDataThread(in: String, dataFile: String): Thread = {
    FileUtil.mkdir(in)
    val dataThread = new Thread(new DataDirectoryServer(in, dataFile))
    dataThread.start()
    dataThread
  }
}
