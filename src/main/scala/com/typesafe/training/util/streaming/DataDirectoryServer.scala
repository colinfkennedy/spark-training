package com.typesafe.training.util.streaming
import com.typesafe.training.util.FileUtil
import java.net.ServerSocket
import java.io.{File, PrintWriter}
import scala.io.Source
import java.io.IOException

/**
 * Serves data to the SparkStreaming example by periodically writing a new
 * file to a watched directory.
 * An alternative to invoking this code as a separate program is to invoke
 * the {@link run} method in a dedicated thread in another application.
 */
class DataDirectoryServer(
  watchDirectory: String, dataFile: String, iterations: Int = Int.MaxValue) extends Runnable {

  import DataDirectoryServer._

  def run: Unit = {
    try {
      println(s"""DataDirectoryServer:
        |  Watch directory: $watchDirectory
        |  Data file:       $dataFile
        |""".stripMargin)
      val directory = openDirectory(watchDirectory)
      val file = openFile(dataFile)
      var count = 1
      while (count < iterations) {
        val outFile = File.createTempFile(s"copy-$count", "txt", directory)
        outFile.deleteOnExit
        val out = new PrintWriter(outFile)
        val source = Source.fromFile(file)
        source.getLines.foreach(out.println)
        source.close
        out.flush
        out.close
        Thread.sleep(sleepInterval)
        count += 1
      }
    } catch {
      case e: IOException =>
        println(s"DataDirectoryServer I/O error: ${e.getMessage}")
        e.printStackTrace()
      case e: InterruptedException =>
        println(s"DataDirectoryServer interrupted. Exiting...")
    }
  }

  protected def openFile(path: String): File = {
    val file = new File(path)
    if (file.isFile == false)
      throw DataDirectoryServerError(s"Input path $path is not a file")
    if (file.exists == false)
      throw DataDirectoryServerError(s"Input path $path does not exist")
    file
  }
  protected def openDirectory(path: String, create: Boolean = true): File = {
    val dir = new File(path)
    if (dir.exists == false) {
      if (create) {
        if (FileUtil.mkdirs(path) == false) {
          throw DataDirectoryServerError(s"Failed to create directory $path")
        }
      } else {
        throw DataDirectoryServerError(s"Input path $path does not exist")
      }
    }
    if (dir.isDirectory == false) {
      throw DataDirectoryServerError(s"Input path $path is not a directory")
    }
    dir
  }
}

object DataDirectoryServer {

  val sleepInterval = 2000 // 2 seconds

  case class DataDirectoryServerError(msg: String) extends RuntimeException(msg)

  /**
   * Usage: DataDirectoryServer watch directory data-file [iterations]
   * where the iterations are the number of times to read the data file
   * and write its contents to the socket. The default is no limit.
   */
  def main(args: Array[String]): Unit = {
    if (args.size < 2) {
      println("DataDirectoryServer: ERROR - Must specify the watch directory and source data file")
    }
    val (watchDirectory, dataFile) = (args(0), args(1))
    val iterations = if (args.length > 2) args(2).toInt else Int.MaxValue
    new DataDirectoryServer(watchDirectory, dataFile, iterations).run
  }
}
