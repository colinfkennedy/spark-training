package com.typesafe.training.sws.ex9

import com.typesafe.training.util.{CommandLineOptions, FileUtil, Printer}
import com.typesafe.training.util.CommandLineOptions.Opt
import com.typesafe.training.sws.KMeansCommandLineOptions
import com.typesafe.training.data._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.functions._  // for min, max, avg, etc.
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import java.io.File

/**
 * Uses K-Means to find clusters in the Divvy stations.
 */
object DivvyKMeans extends DivvyCommon {

  var quiet = false

  def main(args: Array[String]): Unit = {
    val options = CommandLineOptions(
      this, "",
      CommandLineOptions.inputPath(Some(defaultDivvyStationsFile)),
      CommandLineOptions.outputPath(Some(defaultDivvyOutputDir)),
      CommandLineOptions.master(Some(CommandLineOptions.defaultMaster)),
      KMeansCommandLineOptions.k(Some(10)),
      KMeansCommandLineOptions.numberIterations(Some(100)),
      KMeansCommandLineOptions.initializationMode(Some(KMeans.RANDOM)),
      CommandLineOptions.quiet)
    val argz  = options(args.toList)
    val stationsFile  = argz("input-path")

    FileUtil.ls(stationsFile) match {
      case Nil =>
        println(s"Required stations lat-long data ($stationsFile) doesn't exist.")
        println("Please run src/.../ex9/DivvyStations.scala first.")
        // stop
      case _ => runKMeans(argz)
    }
  }

  private def runKMeans(argz: Map[String,String]) = {
    val master        = argz("master")
    val initMode      = argz("initialization-mode")
    val k             = argz("k").toInt
    val numIterations = argz("number-iterations").toInt
    val stationsFile  = argz("input-path")
    val outputDir     = argz("output-path")
    quiet             = argz.getOrElse("quiet", "false").toBoolean

    val sc = new SparkContext(master, "DivvyKMeans")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._  // Needed for column idioms like $"foo".desc.
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")

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

    if (!quiet) {
      val sanity = stationsScaled.toDF("latadj", "longadj").
        agg(min("latadj"), max("latadj"),
            min("longadj"), max("longadj"),
            max("latadj") - min("latadj"),
            max("longadj") - min("longadj")).collect
      println("Sanity check; the last two fields in the following should be ~1.0:")
      sanity.foreach(row => println("  "+row))
    }

    // K-Means wants vectors to train on:
    val stationsScaledVectors = stationsScaled.
      rdd.map(row => Vectors.dense(row.getDouble(0), row.getDouble(1)))

    stationsScaledVectors.cache
    stationsScaledVectors.count

    // Save the scaled coordinates:
    val locScaledPath = outputDir + pathSep + "stations-lat-long-scaled"
    FileUtil.rmrf(locScaledPath)
    saveRDDAsTextFile(stationsScaledVectors, locScaledPath, "Station vectors - scaled")((v:Vector, i:Int) => v(i))

    // Now construct and train the K-Means model on the scaled data.
    val model = new KMeans().
      setInitializationMode(initMode).
      setK(k).
      setMaxIterations(numIterations).
      run(stationsScaledVectors)

    // The cost is a measure of the total distance of points from centroids.
    if (!quiet) {
      val cost = model.computeCost(stationsScaledVectors)
      println(s"Total cost = $cost")
    }

    val centroidsScaled = model.clusterCenters.map(v => (v(0), v(1)))
    val centroids = centroidsScaled.map{
      case (lat, long) => (unscale(lat, latDelta, latAvg), unscale(long, longDelta, longAvg))
    }
    if (!quiet) {
      println(s"Centroids: (very close to (41.90, -87.65))")
      centroids.foreach(c => println("  "+c))
    }

    // Convert each back to an RDD with one partition to use saveAsTextFile:
    val centroidsScaledRDD = sc.parallelize(centroidsScaled, numSlices = 1)
    val centroidsRDD = sc.parallelize(centroids, numSlices = 1)

    val centroidsScaledPath = outputDir + pathSep + s"centroids-$k-adj"
    val centroidsPath = outputDir + pathSep + s"centroids-$k"
    FileUtil.rmrf(centroidsScaledPath)
    FileUtil.rmrf(centroidsPath)

    saveRDDAsTextFile(centroidsScaledRDD, centroidsScaledPath, "Centroids - scaled")(tupGet)
    saveRDDAsTextFile(centroidsRDD, centroidsPath, "Centroids")(tupGet)

    val forPlottingOutFile = outputDir + pathSep + s"for-plotting-$k" + pathSep + "data.csv"
    if (!quiet) println(s"Writing CSV file suitable for spreadsheets: $forPlottingOutFile")
    csvForSpreadSheet(forPlottingOutFile, stationsRDD, centroids)

    printGoogleMapsMessage(stationsFile, centroidsPath)
  }

  /**
   * The second argument "get" knows how to extract field n (counting from 0)
   * from the the record instance of type T.
   */
  protected def saveRDDAsTextFile[T](
    rdd: RDD[T], name: String, message: String)(get: (T, Int) => Double): Unit = {
      if (!quiet) println(s"Writing RDDs $message to $name")
      rdd.coalesce(1).map(t => formatDoubles(get(t,0), get(t,1))).saveAsTextFile(name)
    }

  /**
   * Used in calls to `saveRDDAsTextFile`.
   */
  protected def tupGet(t: (Double,Double), i: Int): Double = if (i==0) t._1 else t._2

  // One way to visualize the data is to import it into a spreadsheet
  // program and use the built-in plot features. The easiest approach
  // is to write a CSV file with the first two columns for the locations
  // and the third and fourth columns for the centroids. Then, plot each
  // pair of columns in the same plot with different point symbols.
  // If you go this route, call the following the function to generate
  // the CSV file.
  protected def csvForSpreadSheet(
    csvFileName: String,
    stationsRDD: RDD[(Double,Double)],
    centroids:   Seq[(Double,Double)]): Unit = {
    // The first k rows will have four columns, while the k+1 to N
    // rows will have just two columns:
    val stats = stationsRDD.collect
    val k = centroids.size
    val forPlotting = stats.take(k).zip(centroids).map {
      case ((d1,d2),(d3,d4)) => formatDoubles(d1,d2,d3,d4)
    }.toVector ++ stats.drop(k).map {
      case (d1,d2) => formatDoubles(d1,d2)
    }
    val csvFile = new File(csvFileName)
    val dir = csvFile.getParentFile
    FileUtil.mkdirs(dir)
    val forPlottingOut = new java.io.PrintWriter(csvFile)
    forPlotting.foreach(forPlottingOut.println)
    forPlottingOut.close()
  }

  protected def printGoogleMapsMessage(
    stationsFile: String, centroidsPath: String): Unit =
    if (!quiet) println(s"""
      |
      |=====================================================================================
      |
      |  To see the stations and centroids on a Chicago map, use Google maps!
      |    1. Open google.com/maps
      |    2. In the upper-left-hand side, select the menu (three horizontal stripes).
      |    3. Select "My Maps".
      |    4. Click the "Create" link. It opens a new map.
      |    5. Click the "import" link under the "Untitled Layer" section.
      |    6. Drag and drop $stationsFile onto the map.
      |    7. Choose the two columns, selecting the 41.78875 column as the latitude
      |       and the -87.60133 column as the longitude.
      |    8. Click "Continue".
      |    9. Select one of the columns for the title. Click Finish.
      |   10. After the data loads, click "Add layer" in the upper-right-hand side.
      |   11. Repeat steps 5-9 with the centroid data, $centroidsPath/part-00000,
      |       but you'll first have to add the ".csv" extension to the file.
      |   12. After the data loads, click the gray icon to the right of the red teardrop
      |       for these points. Change the color and optionally the shape so you can
      |       distinguish these points from the others.
      |   13. Profit!
      |
      |=====================================================================================
      |
      |""".stripMargin)

}
