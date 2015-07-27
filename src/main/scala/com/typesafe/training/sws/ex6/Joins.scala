package com.typesafe.training.sws.ex6

import com.typesafe.training.util.{Timestamp, CommandLineOptions}
import com.typesafe.training.util.CommandLineOptions.Opt
import com.typesafe.training.data._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.typesafe.training.sws.ExtraCommandLineOptions

/**
 * Joins - Perform joins of datasets. Using the airline data, join the
 * origin airport's name using their IATA code. We could/should also join
 * on the destination airport, but we don't to get faster results.
 */
object Joins {
  def main(args: Array[String]): Unit = {

    val options = CommandLineOptions(
      this, "",
      CommandLineOptions.inputPath(Some("data/airline-flights/alaska-airlines/2008.csv")),
      ExtraCommandLineOptions.airports(Some("data/airline-flights/airports.csv")),
      CommandLineOptions.outputPath(Some("output/airline-flights-airports-join")),
      CommandLineOptions.master(Some(CommandLineOptions.defaultMaster)),
      CommandLineOptions.quiet)
    val argz = options(args.toList)
    val quiet = argz.getOrElse("quiet", "false").toBoolean

    val sc = new SparkContext(argz("master"), "Joins")
    try {
      // Load commercial aviation flight data. We'll explore this data set in
      // more depth in subsequent examples. Now, we'll join the flight data with
      // airport data. Note that we convert each line to a Flight object, then
      // project out the origin IATA (airport) code as the key.
      // To reduce the size of the data, just use January's data
      val flights = for {
        line <- sc.textFile(argz("input-path"))
        flight <- Flight.parse(line)
        if flight.date.month == 1
      } yield (flight.origin -> flight)

      // Handle the airports data similarly.
      val airports = for {
        line <- sc.textFile(argz("airports"))
        airport <- Airport.parse(line)
      } yield (airport.iata -> airport.airport)

      // Cache both RDDs in memory for fast, repeated access, if you do
      // multiple joins.
      flights.cache
      airports.cache

      // Join on the key, the first field in the tuples.
      val flights_airports = flights.join(airports)

      if (!quiet) {
        println("flights_airports.toDebugString:")
        println(flights_airports.toDebugString)
      }

      if (flights.count != flights_airports.count) {
        println(s"flights count, ${flights.count}, doesn't match output count, ${flights_airports.count}")
      }

      // Project out reformatted data to flatten the results.
      val flights_airports2 = flights_airports map {
        // Drop the key, the airport iata, because it's already in the flight record.
        // Keep the "value" part, the tuple with the original flight record and the
        // airport name ("airport") appended at the end.
        // "tup" will be (flight, name).
        case (_, tup) => tup
      }

      val now = Timestamp.now()
      val out = s"${argz("output-path")}-$now"
      if (!quiet) println(s"Writing output to: $out")

      flights_airports2.saveAsTextFile(out)
    } finally {
      sc.stop()
    }
  }
}
