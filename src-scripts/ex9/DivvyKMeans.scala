import com.typesafe.training.data._
import com.typesafe.training.util.FileUtil

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.functions._  // for min, max, avg, etc.
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}

sqlContext.setConf("spark.sql.shuffle.partitions", "4")
import sqlContext.implicits._

val initMode = KMeans.RANDOM
val k = 10
val numIterations = 100
val stationsFile = "data/Divvy/stations-lat-long.csv"

val divvyOut = "output/Divvy"
FileUtil.mkdirs(divvyOut)

val stationsRDD = for {
  line <- sc.textFile(stationsFile)
  Array(lat, long) = line.split("\\s*,\\s*").map(_.toDouble)
} yield (lat, long)
val stations = stationsRDD.toDF("latitude", "longitude")

val stationsStatsRow = stations.
  agg(
    avg($"latitude"),  min($"latitude"),  max($"latitude"),
    avg($"longitude"), min($"longitude"), max($"longitude")).
  rdd.collect.head

val Vector(latAvg, latMin, latMax, longAvg, longMin, longMax) = for (
  i <- 0 until stationsStatsRow.size) yield stationsStatsRow.getDouble(i)
val latDelta  = latMax  - latMin
val longDelta = longMax - longMin

// Center at the averages and rescale to +- 0.5.
// It's generally a good idea to scale "features" to be roughly the
// same order of magnitude and centered roughly around zero.
// It can also help prevent floating-point round-off errors for
// some data sets. However, in this case, the effect on the result is
// actually fairly negligible. (Try using the station data without scaling...)
val stationsScaled = stations.
  select(($"latitude" - latAvg ) / latDelta, ($"longitude" - longAvg) / longDelta)

println("Sanity check; the last two fields should be ~1.0:")
stationsScaled.toDF("latadj", "longadj").
  agg(min("latadj"), max("latadj"),
      min("longadj"), max("longadj"),
      max("latadj") - min("latadj"),
      max("longadj") - min("longadj")).collect

// K-Means wants vectors to train on:
val stationsScaledVectors = stationsScaled.
  rdd.map(row => Vectors.dense(row.getDouble(0), row.getDouble(1)))

stationsScaledVectors.cache
stationsScaledVectors.count

// Format each double for output to 5 decimal points.
def formatDoubles(ds: Double*): String =
  ds.toSeq.map(d => f"$d%.5f").mkString(",")

// The second argument "get" knows how to extract field n (counting from 0)
// from the the record instance of type T.
def saveRDDAsTextFile[T](
  rdd: RDD[T], name: String)(get: (T, Int) => Double): Unit =
  rdd.coalesce(1).map(t => formatDoubles(get(t,0), get(t,1))).saveAsTextFile(name)

// Save the scaled coordinates:
val locScaledPath = s"$divvyOut/stations-lat-long-scaled"
FileUtil.rmrf(locScaledPath)
saveRDDAsTextFile(stationsScaledVectors, locScaledPath)((v:Vector, i:Int) => v(i))

// Now construct and train the K-Means model on the scaled data.
val model = new KMeans().
  setInitializationMode(initMode).
  setK(k).
  setMaxIterations(numIterations).
  run(stationsScaledVectors)

// The cost is a measure of the total distance of points from centroids.
val cost = model.computeCost(stationsScaledVectors)
println(s"Total cost = $cost")

def unscaleLat (lat:  Double): Double = (latDelta  * lat)  + latAvg
def unscaleLong(long: Double): Double = (longDelta * long) + longAvg

println(s"Centroids = $centroids")
val centroidsScaled = model.clusterCenters.map(v => (v(0), v(1)))
val centroids = centroidsScaled.map{
  case (lat, long) => (unscaleLat(lat), unscaleLong(long))
}

// Convert each back to an RDD with one partition to use saveAsTextFile:
val centroidsScaledRDD = sc.parallelize(centroidsScaled, numSlices = 1)
val centroidsRDD = sc.parallelize(centroids, numSlices = 1)

val centroidsScaledPath = s"$divvyOut/centroids-$k-adj"
val centroidsPath = s"$divvyOut/centroids-$k"
FileUtil.rmrf(centroidsScaledPath)
FileUtil.rmrf(centroidsPath)

def tupGet(t: (Double,Double), i: Int): Double = if (i==0) t._1 else t._2
saveRDDAsTextFile(centroidsScaledRDD, centroidsScaledPath)(tupGet)
saveRDDAsTextFile(centroidsRDD, centroidsPath)(tupGet)

// One way to visualize the data is to import it into a spreadsheet
// program and use the built-in plot features. The easiest approach
// is to write a CSV file with the first two columns for the locations
// and the third and fourth columns for the centroids. Then, plot each
// pair of columns in the same plot with different point symbols.
// If you go this route, call the following the function to generate
// the CSV file.
val forPlottingOutDir  = s"$divvyOut/for-plotting-$k"
val forPlottingOutFile = "data.csv"
def csvForSpreadSheet(dir: String = forPlottingOutDir, file: String = forPlottingOutFile): Unit = {
  // The first k rows will have four columns, while the k+1 to N
  // rows will have just two columns:
  val stats = stationsRDD.collect
  val forPlotting = stats.take(k).zip(centroids).map {
    case ((d1,d2),(d3,d4)) => formatDoubles(d1,d2,d3,d4)
  }.toVector ++ stats.drop(k).map {
    case (d1,d2) => formatDoubles(d1,d2)
  }
  FileUtil.mkdirs(dir)
  val forPlottingOut = new java.io.PrintWriter(dir+"/"+file)
  forPlotting.foreach(forPlottingOut.println)
  forPlottingOut.close()
}

// However, spreadsheet plots don't show you the Chicago map. For that, use
// Google maps!
//  1. Open google.com/maps
//  2. In the upper-left-hand side, select the menu (three horizontal stripes).
//  3. Select "My Maps".
//  4. Click the "Create" link. It opens a new map.
//  5. Click the "import" link under the "Untitled Layer" section.
//  6. Drag and drop data/Divvy/stations-lat-long.csv onto the map.
//  7. Choose the two columns, selecting the 41.78875 column as the latitude
//     and the -87.60133 column as the longitude.
//  8. Click "Continue".
//  9. Select one of the columns for the title. Click Finish.
// 10. After the data loads, click "Add layer" in the upper-right-hand side.
// 11. Repeat steps 5-9 with the centroid data, output/Divvy/centroids-10/part-00000,
//     but you'll first have to add the ".csv" extension to the file.
// 12. After the data loads, click the gray icon to the right of the red teardrop
//     for these points. Change the color and optionally the shape so you can
//     distinguish these points from the others.
// 13. Profit!
