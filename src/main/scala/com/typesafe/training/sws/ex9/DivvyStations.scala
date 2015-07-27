package com.typesafe.training.sws.ex9

import com.typesafe.training.util.{CommandLineOptions, FileUtil, Printer}
import com.typesafe.training.util.CommandLineOptions.Opt
import com.typesafe.training.data._
import com.typesafe.training.util.FileUtil
import java.io.File
import scala.util.{Try, Success, Failure}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Constructs Divvy station lat-long data in `output/Divvy/stations-lat-long`
 * from the downloaded Divvy data.
 */
object DivvyStations extends DivvyCommon {

  var quiet = false

  def main(args: Array[String]): Unit = {
    val options = CommandLineOptions(
      this, "",
      CommandLineOptions.inputPath(Some(defaultDivvyDir)),
      CommandLineOptions.outputPath(Some(defaultDivvyOutputDir)),
      CommandLineOptions.master(Some("local[1]")),
      CommandLineOptions.quiet)
    val argz  = options(args.toList)
    val inputDir  = argz("input-path")
    val outputDir = argz("output-path")
    val master    = argz("master")
    quiet         = argz.getOrElse("quiet", "false").toBoolean
    setup(inputDir, outputDir, master)
  }

  def setup(inputDir: String, outputDir: String, master: String): Boolean = {
    // Note the unfortunate whitespace that will be in the unzipped contents.
    // If the whitespace causes problems with subsequent usage, remove spaces
    // from this directory manually and change "dataRoot" accordingly.
    val dataRoot =
      inputDir + pathSep + "Data Challenge 2013_2014" + pathSep + "Divvy_Stations_Trips_2013"
    val stationsFile = dataRoot + pathSep + "Divvy_Stations_2013.csv"
    val stationsLatLongDir  = outputDir + pathSep + "stations-lat-long"
    val stationsLatLongFile = stationsLatLongDir + pathSep + "data.csv"

    download(inputDir) && makeStationsLatLong(stationsLatLongFile, stationsFile, master)
  }

  def makeStationsLatLong(targetFileName: String, stationsFile: String, master: String): Boolean = {
    var sc: SparkContext = null
    try {
      val targetFile = new File(targetFileName)
      FileUtil.mkdirs(targetFile.getParentFile())
      FileUtil.rmrf(targetFile)

      sc = new SparkContext(master, "Divvy Stations")

      // Project out the latitude and longitude only as CSV records.
      val stationsLatLong = for {
        line <- sc.textFile(stationsFile)
        station <- Station.parse(line)
        latLong = formatDoubles(station.latitude, station.longitude)
      } yield latLong

      val latLongOut = new java.io.PrintWriter(targetFile)
      stationsLatLong.collect.sortBy(rec => rec).foreach(latLongOut.println)
      latLongOut.close()
      FileUtil.ls(targetFile) match {
        case Nil =>
          println(s"ERROR: Stations file construction appeared to work, but file $targetFileName is not there!")
          false
        case _ =>
          if (!quiet) println(s"Stations file is $targetFileName")
      }
      true
    } catch {
      case scala.util.control.NonFatal(ex) =>
        println("ERROR: Failed to construct the Divvy stations lat-long file: "+ex)
        false
    } finally {
      sc.stop()
    }
  }

  def download(divvyDir: String): Boolean = {
    if (!quiet) print("Checking that Divvy data is already setup... ")
    FileUtil.ls(divvyDir) match {
      case Nil =>
        println(s"Downloading and setting up Divvy data in $divvyDir ... ")
        if (DivvySetup.run() == false) {
          println("ERROR: Call to DivvySetup.run() failed!")
          false
        } else {
          if (!quiet) println("Setup succeeded!")
          true
        }
      case _ =>
        if (!quiet) println("yes")
        true
    }
  }
}

