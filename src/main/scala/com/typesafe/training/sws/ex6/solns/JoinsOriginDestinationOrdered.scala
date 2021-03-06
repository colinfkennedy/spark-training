package com.typesafe.training.sws.ex6.solns

import com.typesafe.training.util.{Timestamp, CommandLineOptions}
import com.typesafe.training.util.CommandLineOptions.Opt
import com.typesafe.training.data._
import com.typesafe.training.data.Flight._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.typesafe.training.sws.ExtraCommandLineOptions

/**
 * Joins - Perform joins of datasets. Using the airline data, join the
 * origin and destination airports' names using their IATA codes.
 * This solution also sorts by timestamp.
 */
object JoinsOriginDestinationOrdered {
  def main(args: Array[String]): Unit = {

    val options = CommandLineOptions(
      this, "",
      CommandLineOptions.inputPath(Some("data/airline-flights/alaska-airlines/2008.csv")),
      ExtraCommandLineOptions.airports(Some("data/airline-flights/airports.csv")),
      CommandLineOptions.outputPath(Some("output/airline-flights-airports-join-origin-dest-ordered")),
      CommandLineOptions.master(Some(CommandLineOptions.defaultMaster)),
      CommandLineOptions.quiet)
    val argz = options(args.toList)

    // New: Order the flights by dates and times.
    // By declaring this object "implicit", it will be used automatically by
    // sortByKey() below. (See the Scaladocs for sortByKey).
    implicit object FlightOrdering extends Ordering[Flight] {
      def compare(a: Flight, b: Flight): Int = {
        val diff1 = a.date compare b.date
        if (diff1 != 0) diff1
        else a.times compare b.times
      }
    }

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

      // Join on the origin airport first using the keys, the first fields
      // in the tuples.
      val flights_airports1 = flights.join(airports)

      if (flights.count != flights_airports1.count) {
        println(s"flights count, ${flights.count}, doesn't match output count, ${flights_airports1.count}")
      }

      // Now use the destination airport for the key, but first project out the
      // the join tuple as in previous versions of this example. We'll use a
      // simpler form of the anonymous function for this.
      val flights_airports2 = flights_airports1
        .map(tup => tup._2)  // vs. map { case (_, tup) => tup }
        .keyBy(tuple => tuple._1.dest)  // tuple is (flight, airport_name)

      // Join again.
      val flights_airports3 = flights_airports2.join(airports)

      // Project out reformatted data to flatten the results
      val flights_airports = flights_airports3.map {
        case (_, ((flight, originName), destinationName)) =>
          (flight, (originName, destinationName)) // nested tuple so sorting works.
      }
      // New: sort by the flight, using the implicit ordering above.
      .sortByKey()
      // ... and project out the final form.
      .map {
        case (flight, (o, d)) => (flight, o, d)
      }

      val now = Timestamp.now()
      val out = s"${argz("output-path")}-$now"
      if (! argz.getOrElse("quiet", "false").toBoolean)
        println(s"Writing output to: $out")

      flights_airports.saveAsTextFile(out)
    } finally {
      sc.stop()
    }
  }
}
