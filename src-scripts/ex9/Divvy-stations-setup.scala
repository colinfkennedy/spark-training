// this file was used to construct the included Divvy Station lat-long
// data in `data/Divvy/stations-lat-long.csv` from the actual data available
// at https://www.divvybikes.com/datachallenge. You'll need to download the
// data from there to run this script. Edit the definition of "dataRoot"
// below before running.

import com.typesafe.training.data._
import com.typesafe.training.util.FileUtil
import org.apache.spark.rdd.RDD

sqlContext.setConf("spark.sql.shuffle.partitions", "4")

// Change this value to point to your directory for the downloaded data:
val dataRoot = "/Users/deanwampler/Documents/data/Divvy_Stations_Trips_2013"
val stationsFile = dataRoot+"/Divvy_Stations_2013.csv"

val divvy = "data/Divvy"
FileUtil.mkdirs(divvy)
val stationsPath = s"$divvy/stations-lat-long.csv"

// For nicely formatting a variable argument list of Double values
// to 5 decimal places.
def formatDoubles(ds: Double*): String =
  ds.toSeq.map(d => f"$d%.5f").mkString(",")

// Project out the latitude and longitude only as CSV records.
val stationsLatLong = for {
  line <- sc.textFile(stationsFile)
  station <- Station.parse(line)
  latLong = formatDoubles(station.latitude, station.longitude)
} yield latLong

val latLongOut = new java.io.PrintWriter(stationsPath)
stationsLatLong.collect.sortBy(rec => rec).foreach(latLongOut.println)
latLongOut.close()

