package com.typesafe.training.sws.ex9

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.typesafe.training.data._
import com.typesafe.training.util.{FileUtil, ZipUtil}
import java.io.File
import scala.util.{Try, Success, Failure}

/**
 * Constructs Divvy station lat-long data in `output/Divvy/stations-lat-long`
 * from the downloaded Divvy data
 */
object DivvyStations {

  def main(args: Array[String]): Unit = {
    val success = if (setup()) 0 else 1
    sys.exit(success)
  }

  val sep = File.separator
  val divvyDirName  = "data/Divvy"
  val outputDirName = "output/Divvy"

  def setup(inputDir: String = divvyDirName, outputDir: String = outputDirName): Boolean = {
    // Note the unfortunate whitespace that will be in the unzipped contents.
    // If the whitespace causes problems with subsequent usage, remove spaces
    // from this directory manually and change "dataRoot" accordingly.
    val dataRoot =
      inputDir + sep + "Data Challenge 2013_2014" + sep + "Divvy_Stations_Trips_2013"
    val stationsFile = dataRoot + sep + "Divvy_Stations_2013.csv"
    val stationsLatLongDir  = outputDir + sep + "stations-lat-long"
    val stationsLatLongFile = stationsLatLongDir + sep + "data.csv"

    download(inputDir) && makeStationsLatLong(stationsLatLongFile, stationsFile)
  }

  def makeStationsLatLong(targetFileName: String, stationsFile: String): Boolean = try {
    val targetFile = new File(targetFileName)
    FileUtil.mkdirs(targetFile.getParentFile())
    FileUtil.rmrf(targetFile)

    val sc = new SparkContext("local[1]", "Divvy Stations")

    // Project out the latitude and longitude only as CSV records.
    val stationsLatLong = for {
      line <- sc.textFile(stationsFile)
      station <- Station.parse(line)
      latLong = formatDoubles(station.latitude, station.longitude)
    } yield latLong

    val latLongOut = new java.io.PrintWriter(targetFile)
    stationsLatLong.collect.sortBy(rec => rec).foreach(latLongOut.println)
    latLongOut.close()
    true
  } catch {
    case scala.util.control.NonFatal(ex) =>
      println("Failed to construct the Divvy stations lat-long file: "+ex)
      false
  }

  def download(divvyDir: String): Boolean = {
    print("Checking that Divvy data is already setup... ")
    FileUtil.ls(divvyDir) match {
    case Nil =>
      println(s"Downloading and setting up Divvy data in $divvyDir ... ")
      if (DivvySetup.run() == false) {
        println("Setup failed!")
        false
      } else {
        println("Setup succeeded!")
        true
    case _ =>
      println("yes")
      true
  }

  // For nicely formatting a variable argument list of Double values
  // to 5 decimal places.
  def formatDoubles(ds: Double*): String =
    ds.toSeq.map(d => f"$d%.5f").mkString(",")

}

