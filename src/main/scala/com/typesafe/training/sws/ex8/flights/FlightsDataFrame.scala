package com.typesafe.training.sws.ex8.flights

import com.typesafe.training.data._
import com.typesafe.training.util.Timestamp
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import java.io.PrintStream

/**
 * A demonstration of Spark Streaming, reading airline data either over a
 * socket or from files, and using the DataFrame API to analyze it.
 */
object FlightsDataFrame extends FlightsCommon {

  def main(args: Array[String]): Unit = initMain(args)

  protected def computeFlightDelays(flights: DStream[Flight]): Unit = {

    // Compute the total elapsed minutes of flights per year-month.
    val totalElapsedMinutes = flights.map { flight =>
        ((flight.date.year, flight.date.month), flight.times.crsElapsedTime)
      }
      .reduceByKey(_ + _)
    if (!quiet) {
      totalElapsedMinutes.foreachRDD { rdd =>
        println("\ntotalElapsedMinutes:")
        rdd foreach println
      }
    }


    // Filter for delayed flights and compute the total delayed minutes for
    // each kind of delay per year-month.
    val delays = flights.filter { flight =>
        flight.carrierDelay      > 0 ||
        flight.weatherDelay      > 0 ||
        flight.nasDelay          > 0 ||
        flight.lateAircraftDelay > 0
      }
      // Setup for reduceByKey:
      .map { flight =>
        ((flight.date.year, flight.date.month),
         (flight.carrierDelay,
          flight.weatherDelay,
          flight.nasDelay,
          flight.lateAircraftDelay))
      }
      .reduceByKey {
        case ((cd1, wd1, nasd1, lad1), (cd2, wd2, nasd2, lad2)) =>
          (cd1 + cd2, wd1 + wd2, nasd1 + nasd2, lad1 + lad2)
      }
    if (!quiet) {
      delays.foreachRDD { rdd =>
        println("\ndelays:")
        rdd foreach println
      }
    }

    // Average the delay totals by the total elapses flight minutes
    // per year-month. Show as a percentage.
    val scaledDelays = delays.join(totalElapsedMinutes)
      .map {
        case ((year, month), ((cd1, wd1, nasd1, lad1), elapsed)) =>
          ((year, month),
           (cd1*100.0/elapsed, wd1*100.0/elapsed, nasd1*100.0/elapsed, lad1*100.0/elapsed))
      }

    scaledDelays.foreachRDD { rdd =>
      printDelaysHeader(rdd.id)
      rdd.coalesce(1).sortByKey().map(formatRecord).foreach(println)
      printDelaysTrailer()
    }
  }

  protected def computeFlightsBetweenAirports(
    flights: DStream[Flight], airportsOutPath: String, window: Duration, slide: Duration): Unit = {

    val countOrderer = new Ordering[((String, String), Long)] {
      def compare(a: ((String, String), Long), b: ((String, String), Long)): Int =
        a._2 compare b._2
    }

    flights.window(window, slide)
      .map(flight => ((flight.origin, flight.dest), 1L))
      .reduceByKey(_ + _)
      .foreachRDD { rdd =>
        val now = Timestamp.now()
        if (!quiet) println(s"$now: writing airport pairs...\n")
        val str = rdd.top(20)(countOrderer).mkString(s"$now: ", ", ", "")
        airportsOut(airportsOutPath).println(str)
      }
  }
}