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
 * The "backend" program that generates data for the SparkStreaming example.
 * It provides the source data either over a socket or by periodically writing
 * new data files to a directory.
 * Run with the --help option for details.
 */
object SparkStreamingBackend {

  /**
   * The source data file to write over a socket or to write repeatedly to the
   * directory specified by --watch-directory.
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

  def main(args: Array[String]): Unit = {
    val options = CommandLineOptions(
      this, "Use --socket or --watch-directory (default) arguments",
      // For this process, use at least 2 cores!
      CommandLineOptions.master(Some(CommandLineOptions.defaultMaster)),
      CommandLineOptions.socket(None),  // empty default, so we know the user specified this option.
      SparkStreamingCommon.WatchDirectory(Some(SparkStreamingCommon.WatchDirectory.default)),
      sourceDataFile(Some("data/airline-flights/alaska-airlines/2008.csv")),
      CommandLineOptions.quiet)
    val argz   = options(args.toList)

    val master   = argz("master")
    val quiet    = argz.getOrElse("quiet", "false").toBoolean
    val watchDir = argz("watch-directory")
    val data     = argz("source-data-file")
    val socket   = argz.getOrElse("socket", "")

    try {
      val dataThread =
        if (socket == "") {
          startDirectoryDataThread(watchDir, data)
        } else {
          val port = socket.split(":").last.toInt
          startSocketDataThread(port, data)
        }
        println("*** Hit 'enter' to quit at any time.")
        Console.in.read()
        dataThread.interrupt()
    } finally {
      FileUtil.rmrf(watchDir)
    }
  }

  def startSocketDataThread(port: Int, dataFile: String): Thread = {
    val dataThread = new Thread(new DataSocketServer(port, dataFile))
    dataThread.start()
    dataThread
  }
  def startDirectoryDataThread(watchDir: String, dataFile: String): Thread = {
    FileUtil.mkdirs(watchDir)
    val dataThread = new Thread(new DataDirectoryServer(watchDir, dataFile))
    dataThread.start()
    dataThread
  }
}
