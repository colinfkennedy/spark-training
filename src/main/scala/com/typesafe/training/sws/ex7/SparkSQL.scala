package com.typesafe.training.sws.ex7

import com.typesafe.training.util.{CommandLineOptions, Printer}
import com.typesafe.training.util.CommandLineOptions.Opt
import com.typesafe.training.util.sql.SparkSQLRDDUtil
import com.typesafe.training.sws.ExtraCommandLineOptions
import com.typesafe.training.data._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.rdd.RDD

/**
 * Example of Spark SQL.
 * Writes "query" results to the console, rather than a file.
 */
object SparkSQL {

  var out = Console.out   // Overload for tests

  var sqlContext: SQLContext = null
  var quiet = false

  def main(args: Array[String]): Unit = {

    // We'll use Alaska Airlines data to reduce the size of the data set.
    val options = CommandLineOptions(
      this, "",
      CommandLineOptions.inputPath(Some("data/airline-flights/alaska-airlines/2008.csv")),
      ExtraCommandLineOptions.carriers(Some("data/airline-flights/carriers.csv")),
      ExtraCommandLineOptions.airports(Some("data/airline-flights/airports.csv")),
      ExtraCommandLineOptions.planes(Some("data/airline-flights/plane-data.csv")),
      CommandLineOptions.master(Some(CommandLineOptions.defaultMaster)),
      CommandLineOptions.quiet)
    val argz  = options(args.toList)

    quiet = argz.getOrElse("quiet", "false").toBoolean

    val sc = new SparkContext(argz("master"), "Spark SQL")
    sqlContext = new SQLContext(sc)
    // Change to a more reasonable default number of partitions (from 200)
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")

    try {
      // Use a pattern match to extract the four RDDs into named variables:
      val (flights, carriers, airports, planes) =
        SparkSQLRDDUtil.load(sc,
          argz("input-path"),
          argz("carriers"),
          argz("airports"),
          argz("planes"))

      register(flights,  "flights")
      register(carriers, "carriers")
      register(airports, "airports")
      register(planes,   "planes")

      // Write some queries!!

      println(s"total_flights = ${flights.count}")

      val canceled_flights = sqlContext.sql(
        "SELECT COUNT(*) FROM flights f WHERE f.canceled > 0")
      Printer(out, "canceled flights", canceled_flights)
      if (!quiet) {
        println("\ncanceled_flights.explain(extended = false):")
        canceled_flights.explain(extended = false)
        println("\ncanceled_flights.explain(extended = true):")
        canceled_flights.explain(extended = true)
      }

      // NOTE: If we registered canceled_flights as a table, we could use it
      // and eliminate the WHERE clause.
      val canceled_flights_by_month = sqlContext.sql("""
        SELECT f.date.month AS month, COUNT(*)
        FROM flights f
        WHERE f.canceled > 0
        GROUP BY f.date.month
        ORDER BY month""")
      Printer(out, "canceled flights by month", canceled_flights_by_month)
      if (!quiet) {
        println("\ncanceled_flights_by_month.explain(extended = false):")
        canceled_flights_by_month.explain(extended = false)
        println("\ncanceled_flights_by_month.explain(extended = true):")
        canceled_flights_by_month.explain(extended = true)
      }

      val flights_between_airports = sqlContext.sql("""
        SELECT origin, dest, COUNT(*)
        FROM flights
        GROUP BY origin, dest
        ORDER BY origin, dest""")
      Printer(out, "Flights between airports, sorted by airports", flights_between_airports)
      if (!quiet) {
        println("\nflights_between_airports.explain(true):")
        flights_between_airports.explain(true)
      }

      // Unfortunately, SparkSQL's SQL dialect doesn't yet support column aliasing
      // for function outputs, which we would like to use for "COUNT(*) as count",
      // then "ORDER BY count". However, we can use the synthesized name, c2.
      val flights_between_airports2 = sqlContext.sql("""
        SELECT origin, dest, COUNT(*)
        FROM flights
        GROUP BY origin, dest
        ORDER BY c2 DESC""")
      // There are ~170, so print them all, but it uses ~200 partitions!
      // This inefficiency can't be fixed when using SQL, but can be fixed
      // when using the DataFrames DSL; see SparkDataFrame.scala.
      Printer(out, "Flights between airports, sorted by counts", flights_between_airports2, 1000)
      if (!quiet) {
        println("\nflights_between_airports2.explain(true):")
        flights_between_airports2.explain(true)
      }
    } finally {
      sc.stop()
    }
  }

  import scala.reflect.runtime.universe.TypeTag

  /**
   * Register the RDDs as temporary "tables".
   */
  def register[T <: Product : TypeTag](rdd: RDD[T], name: String): Unit = {
    val df = sqlContext.createDataFrame(rdd)
    df.registerTempTable(name)
    df.cache()
    if (! quiet) {
      println(s"Schema for $name:")
      df.printSchema()
    }
  }
}
