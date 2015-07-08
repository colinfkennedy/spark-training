package com.typesafe.training.sws.ex6.solns

import com.typesafe.training.data.Airport
import com.typesafe.training.sws.ExtraCommandLineOptions
import com.typesafe.training.util.CommandLineOptions
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkContext}
import org.apache.spark.rdd.RDD

object DataShufflingWithPartitioner {

  val numIterations = 5
  val sleepInterval = 5000   // milliseconds
  val airportPrefix = "S"

  def main(args: Array[String]): Unit = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName, "",
      ExtraCommandLineOptions.airports(Some("data/airline-flights/airports.csv")),
      CommandLineOptions.master(Some("local[2]")),
      CommandLineOptions.quiet)
    val argz = options(args.toList)

    implicit val sc = new SparkContext(argz("master"), "data-partitioning")
    try {
      val airports = for {
        line <- sc.textFile(argz("airports"))
        airport <- Airport.parse(line)
      } yield airport.iata -> airport.airport

      // Just the IATA
      val airportCodesSample = airports.map(tup => tup._1).takeSample(true, 1000)

      // New: Control how partitioning works.
      val partitioned = airports.partitionBy(new HashPartitioner(partitions = 4)).cache()
      // val partitioned = airports.partitionBy(new RangePartitioner(partitions = 4, rdd = airports)).cache()

      processFlightLog(partitioned, airportCodesSample)

    } finally {
      sc.stop()
    }
  }

  def processFlightLog(
    airports: RDD[(String, String)], airportCodesSample: Array[String])(
    implicit sc: SparkContext): Unit = {
    processFlightEveryFiveSeconds(airportCodesSample) { flightLog =>
      val joined = airports.join(flightLog)
      val c = joined.filter {
        case (origin, (airport, (dest, flightData))) => dest.startsWith(airportPrefix)
      }.count()

      println(s"Total count of flights leaving for airport starting with '$airportPrefix' = " + c)
    }
  }

  def processFlightEveryFiveSeconds(
    airportCodesSample: Array[String])(
    callback: (RDD[(String, (String, String))]) => Unit)(
    implicit sc: SparkContext): Unit = {

    (1 to numIterations) foreach { _ =>
      // Use a sample of all the airport codes and generate a RDD
      // with random pairs of airport codes.
      val log = generateFlightLog(airportCodesSample)
      callback(log)
      Thread.sleep(sleepInterval) // Poor man's way to delay work
    }
  }

  // Generate random flight data (origin_airport, (dest_airport, flight_data))
  // out of the given array of airport codes
  private def generateFlightLog(codes: Array[String])(implicit sc: SparkContext): RDD[(String, (String, String))] = {
    import scala.util.Random._
    sc.parallelize((1 to 50) map { i =>
      val origin = codes(nextInt(codes.length))
      val dest = codes(nextInt(codes.length))
      (origin, (dest, s"Random flight data $i"))
    })
  }
}
