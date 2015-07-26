// This file downloads the Divvy data set and then constructs the
// Divvy Station lat-long data in `output/Divvy/stations-lat-long.csv` from the actual data available
// at https://www.divvybikes.com/datachallenge, which this script downloads.
// Hence, you'll need an internet connection to run this script.

import com.typesafe.training.data._
import com.typesafe.training.util.{FileUtil, ZipUtil}
import com.typesafe.training.util.www.Curl
import java.io.File
import scala.util.{Try, Success, Failure}

val sep       = File.separator
val divvyURL  = "https://s3.amazonaws.com/divvy-data/datachallenge/datachallenge.zip"
val zipName   = divvyURL.split("/").last
val divvyDir  = "data/Divvy"
val zipFile   = divvyDir + sep + zipName
val outputDir = "output/Divvy"
// Note the unfortunate whitespace that will be in the unzipped contents.
// If the whitespace causes problems with subsequent usage, remove spaces
// from this directory manually and change "dataRoot" accordingly.
val dataRoot =
  divvyDir + sep + "Data Challenge 2013_2014" + sep + "Divvy_Stations_Trips_2013"
val stationsFile = dataRoot + sep + "Divvy_Stations_2013.csv"
val stationsLatLongDir  = outputDir + sep + "stations-lat-long"
val stationsLatLongFile = stationsLatLongDir + sep + "data.csv"

// For nicely formatting a variable argument list of Double values
// to 5 decimal places.
def formatDoubles(ds: Double*): String =
  ds.toSeq.map(d => f"$d%.5f").mkString(",")

def makeStationsLatLong() = {
  FileUtil.mkdirs(stationsLatLongDir)
  FileUtil.rmrf(stationsLatLongFile)

  // Project out the latitude and longitude only as CSV records.
  val stationsLatLong = for {
    line <- sc.textFile(stationsFile)
    station <- Station.parse(line)
    latLong = formatDoubles(station.latitude, station.longitude)
  } yield latLong

  val latLongOut = new java.io.PrintWriter(stationsLatLongFile)
  stationsLatLong.collect.sortBy(rec => rec).foreach(latLongOut.println)
  latLongOut.close()
}

def download(): Boolean = Curl(divvyURL, divvyDir, showProgress = true) match {
  case Failure(ex) =>
    Console.err.println(s"Curl failed: $ex")
    false
  case Success(divvyDir) =>
    ZipUtil.unzip(zipFile, divvyDir) match {
      case Failure(ex) =>
        Console.err.println(s"Curl failed: $ex")
        false
      case Success(divvyDir) =>
        true
    }
}

// If you've already downloaded the data successfully, skip calling download().
if (download()) {
  makeStationsLatLong()
  FileUtil.ls(stationsLatLongDir) match {
    case Nil => println("Failed to create Station lat-long output: "+stationsLatLongFile)
    case seq =>
      println(s"Contents of $stationsLatLongDir:")
      seq foreach (f => println("  "+f))
  }
}

