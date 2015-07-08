package com.typesafe.training.sws.ex7

import com.typesafe.training.util.{CommandLineOptions, Printer}
import com.typesafe.training.util.CommandLineOptions.Opt
import com.typesafe.training.util.sql.SparkSQLRDDUtil
import com.typesafe.training.sws.ExtraCommandLineOptions
import com.typesafe.training.data._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

/**
 * Example of Spark DataFrames, a Python Pandas-like API built on top of
 * SparkSQL with better performance than RDDs, due to the use of Catalyst
 * for "query" optimization.
 */
object SparkDataFrames {

  var out = Console.out   // Overload for tests
  var quiet = false

  def main(args: Array[String]): Unit = {

    // We'll use Alaska Airlines data to reduce the size of the data set.
    val options = CommandLineOptions(
      this.getClass.getSimpleName, "",
      CommandLineOptions.inputPath(Some("data/airline-flights/alaska-airlines/2008.csv")),
      ExtraCommandLineOptions.carriers(Some("data/airline-flights/carriers.csv")),
      ExtraCommandLineOptions.airports(Some("data/airline-flights/airports.csv")),
      ExtraCommandLineOptions.planes(Some("data/airline-flights/plane-data.csv")),
      ExtraCommandLineOptions.tungstenMode,
      CommandLineOptions.master(Some("local[*]")),
      CommandLineOptions.quiet)
    val argz  = options(args.toList)

    quiet = argz.getOrElse("quiet", "false").toBoolean

    val conf = new SparkConf()
    conf.setMaster(argz("master"))
    conf.setAppName("Spark SQL DataFrames")
    // Turn on some experimental "Project Tungsten" features?
    if (argz.getOrElse("tungstenMode", "false").toBoolean) {
      conf.set("spark.sql.codegen", "true")
      conf.set("spark.sql.unsafe.enabled", "true")
      conf.set("spark.shuffle.manager", "tungsten-sort")
    }
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._  // Needed for column idioms like $"foo".desc.

    try {
      // Use pattern matching tricks to extract the elements from the tuple
      // of RDDs returned by SparkSQLRDDUtil, and then from a Seq used to
      // iterate over the four RDDs to convert to DataFrames:
      val (flightsRDD, carriersRDD, airportsRDD, planesRDD) =
        SparkSQLRDDUtil.load(sc,
          argz("input-path"),
          argz("carriers"),
          argz("airports"),
          argz("planes"))
      val flights  = sqlContext.createDataFrame(flightsRDD)
      val carriers = sqlContext.createDataFrame(carriersRDD)
      val airports = sqlContext.createDataFrame(airportsRDD)
      val planes   = sqlContext.createDataFrame(planesRDD)
      if (!quiet) {
        Seq(flights, carriers, airports, planes) foreach { df =>
          df.printSchema
          df.show()  // show() prints 20 records in the DataFrame.
        }
      }
      // Cache just the flights and airports.
      flights.cache
      airports.cache

      // If you want to rename the columns, you can create a new DataFrame
      // with toDF. There is also an overload of `SQLContext#createDataFrame`
      // that lets you do this from an RDD.
      if (!quiet) {
        carriers.printSchema
        val c2 = carriers.toDF("abbreviation", "name")
        c2.printSchema
      }

      // Write some "queries" using the DataFrame API.
      // We'll show analogous SQL queries that are used in SparkSQL.scala,
      // pretending that we have "tables" with the same names as the DataFrame
      // variable names.

      if (!quiet) {
        out.println(s"total_flights = ${flights.count}")
      }

      // SELECT COUNT(*) FROM flights f WHERE f.canceled > 0;
      val canceled_flights = flights.filter(flights("canceled") > 0)
      Printer(out, "canceled flights", canceled_flights)
      if (!quiet) {
        out.println("\ncanceled_flights.explain(extended = false):")
        canceled_flights.explain(extended = false)
        out.println("\ncanceled_flights.explain(extended = true):")
        canceled_flights.explain(extended = true)
      }
      canceled_flights.cache

      // Note how we can reference the columns several ways:
      if (!quiet) {
        flights.orderBy(flights("origin")).show
        flights.orderBy("origin").show
        flights.orderBy($"origin").show
        flights.orderBy($"origin".desc).show
      }
      // The last one $"count".desc is the only (?) way to specify descending order.
      // The $"..." is not a Scala built-in feature, but Scala allows you to
      // implement "interpolated string" handlers with your own prefix ($ in
      // this case).

      // SELECT cf.date.month AS month, COUNT(*)
      //   FROM canceled_flights cf
      //   GROUP BY cf.date.month
      //   ORDER BY month;
      val canceled_flights_by_month = canceled_flights.
        groupBy("date.month").count()
      Printer(out, "canceled flights by month", canceled_flights_by_month)
      if (!quiet) {
        out.println("\ncanceled_flights_by_month.explain(true):")
        canceled_flights_by_month.explain(true)
      }
      canceled_flights.unpersist

      // The use of coalesce(4) turns ~200 partitions int 4,
      // greatly improving performance!
      // SELECT origin, dest, COUNT(*)
      //   FROM flights
      //   GROUP BY origin, dest
      //   ORDER BY origin, dest;
      val flights_between_airports = flights.
        groupBy("origin", "dest").count().coalesce(4).
        orderBy("origin", "dest")
      Printer(out, "Flights between airports, sorted by airports", flights_between_airports)
      if (!quiet) {
        out.println("\nflights_between_airports.explain(true):")
        flights_between_airports.explain(true)
      }
      flights_between_airports.cache

      // The call to coalesce(4) seems redundant, but apparently isn't.
      // If you remove it, `orderBy` processes ~170 partitions.
      // SELECT origin, dest, COUNT(*)
      //   FROM flights_between_airports
      //   ORDER BY count DESC;
      val frequent_flights_between_airports = flights_between_airports.
        coalesce(4).orderBy($"count".desc)
      // Show all of them (~170)
      Printer(out, "Flights between airports, sorted by counts descending", frequent_flights_between_airports, 200)
      if (!quiet) {
        out.println("\nfrequent_flights_between_airports.explain(true):")
        frequent_flights_between_airports.explain(true)
      }
    } finally {
      sc.stop()
    }
  }
}
