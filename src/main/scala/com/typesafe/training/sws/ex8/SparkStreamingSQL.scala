package com.typesafe.training.sws.ex8

import com.typesafe.training.data._
import com.typesafe.training.util.Timestamp
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.{Row, SQLContext}
import java.io.PrintStream

/**
 * A demonstration of Spark Streaming combined with SparkSQL, using airline data.
 * In production applications, you should probably use DataFrames instead.
 * Try porting this code to use DataFrames.
 */
object SparkStreamingSQL extends SparkStreamingCommon {

  def main(args: Array[String]): Unit = initMain(args)

  protected def computeFlightDelays(flights: DStream[Flight]): Unit = {

    val sqlContext: SQLContext = new SQLContext(flights.context.sparkContext)
    // Change to a more reasonable default number of partitions (from 200)
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")

    flights.foreachRDD { flights =>
      sqlContext.createDataFrame(flights).registerTempTable("flights")

      // Compute the total elapsed minutes of flights per year-month.
      val totalElapsedMinutes = sqlContext.sql("""
        SELECT date.year AS year, date.month AS month,
        SUM(times.crsElapsedTime) as total
        FROM flights
        GROUP BY date.year, date.month""")
      totalElapsedMinutes.coalesce(4).cache

      totalElapsedMinutes.registerTempTable("elapsed")
      if (!quiet) {
        println("\n===== totalElapsedMinutes:")
        totalElapsedMinutes.printSchema
        totalElapsedMinutes foreach println
      }

      // Filter for delayed flights and compute the total delayed minutes for
      // each kind of delay per year-month.
      val delays = sqlContext.sql("""
        SELECT date.year AS year, date.month AS month,
          SUM(carrierDelay)      AS carrierDelay,
          SUM(weatherDelay)      AS weatherDelay,
          SUM(nasDelay)          AS nasDelay,
          SUM(lateAircraftDelay) AS lateAircraftDelay
        FROM flights
        WHERE carrierDelay > 0 OR weatherDelay > 0 OR nasDelay > 0 OR lateAircraftDelay > 0
        GROUP BY date.year, date.month""")
      delays.coalesce(4).cache

      delays.registerTempTable("delays")
      if (!quiet) {
        println("\n===== delays:")
        delays.printSchema
        delays foreach println
      }

      // Average the delay totals by the total elapses flight minutes
      // per year-month. Show as a percentage. This query has terrible performance!
      val scaledDelays = sqlContext.sql("""
        SELECT d.year AS year, d.month AS month,
          (d.carrierDelay      * 100.0) / (e.total * 1.0) AS carrierDelay,
          (d.weatherDelay      * 100.0) / (e.total * 1.0) AS weatherDelay,
          (d.nasDelay          * 100.0) / (e.total * 1.0) AS nasDelay,
          (d.lateAircraftDelay * 100.0) / (e.total * 1.0) AS lateAircraftDelay
        FROM delays d
        JOIN elapsed e ON d.year = e.year AND d.month = e.month""")
        .map { row =>
           // Convert to the expected format:
          ((row.getInt(0), row.getInt(1)), (row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getDouble(5)))
        }
        .cache

        printDelaysHeader(scaledDelays.id)
        scaledDelays.coalesce(1).sortByKey().map(formatRecord).foreach(println)
        printDelaysTrailer()
    }
  }

  protected def computeFlightsBetweenAirports(
    flights: DStream[Flight], airportsOutPath: String, window: Duration, slide: Duration): Unit = {

    val sqlContext: SQLContext = new SQLContext(flights.context.sparkContext)

    flights.window(window, slide)
      .foreachRDD { flights =>
        sqlContext.createDataFrame(flights).registerTempTable("flights")
        val flights_between_airports = sqlContext.sql("""
          SELECT origin, dest, COUNT(*)
          FROM flights
          GROUP BY origin, dest
          ORDER BY c2 DESC
          LIMIT 20""")
        // Must convert a Row object to the expected ((String,String), Long) format:
        val fba = flights_between_airports.collect map { row =>
          ((row.getString(0), row.getString(1)), row.getLong(2))
        }
        val now = Timestamp.now()
        if (!quiet) println(s"$now: writing airport pairs...\n")
        val str = fba.mkString(s"$now: ", ", ", "")
        airportsOut(airportsOutPath).println(str)
      }
  }
}
