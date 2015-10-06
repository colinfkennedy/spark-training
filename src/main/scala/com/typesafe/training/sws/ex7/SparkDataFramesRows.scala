package com.typesafe.training.sws.ex7

import com.typesafe.training.util.{CommandLineOptions, Printer}
import com.typesafe.training.util.CommandLineOptions.Opt
import com.typesafe.training.sws.ExtraCommandLineOptions
import com.typesafe.training.data._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType, FloatType}

/**
 * An example of using the Row idiom with input text data when constructing
 * a DataFrame. This is an alternative approach to using case classes, as in
 * `SparkDataFrames`.
 */
object SparkDataFramesRows {

  var out = Console.out   // Overload for tests
  var quiet = false

  def main(args: Array[String]): Unit = {

    // We'll use just the Airport data to illustrate the idiom.
    val options = CommandLineOptions(
      this, "",
      ExtraCommandLineOptions.airports(Some("data/airline-flights/airports.csv")),
      ExtraCommandLineOptions.tungstenMode,
      CommandLineOptions.master(Some(CommandLineOptions.defaultMaster)),
      CommandLineOptions.quiet)
    val argz  = options(args.toList)

    quiet = argz.getOrElse("quiet", "false").toBoolean

    val conf = new SparkConf()
    conf.setMaster(argz("master"))
    conf.setAppName("Spark SQL DataFrames with Rows")
    // Turn on some experimental "Project Tungsten" features?
    if (argz.getOrElse("tungstenMode", "false").toBoolean) {
      conf.set("spark.sql.codegen", "true")
      conf.set("spark.sql.unsafe.enabled", "true")
      conf.set("spark.shuffle.manager", "tungsten-sort")
    }
    // Change to a more reasonable default number of partitions for our data
    // (from 200)
    conf.set("spark.sql.shuffle.partitions", "4")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._  // Needed for column idioms like $"foo".desc.

    try {

      val schema = StructType(
        StructField("iata",      StringType, nullable = false) +:    // 1: IATA code
        StructField("airport",   StringType, nullable = false) +:    // 2: Name
        StructField("city",      StringType, nullable = false) +:    // 3: City
        StructField("state",     StringType, nullable = false) +:    // 4: State
        StructField("country",   StringType, nullable = false) +:    // 5: Country
        StructField("latitude",  FloatType,  nullable = false) +:    // 6: Latitude
        StructField("longitude", FloatType,  nullable = false) +:    // 7: Longitude
        Nil)

      // Copied from Airport.parse in Airline.scala.
      def parse(s: String): Option[Row] = s match {
        case Airport.headerRE() => None
        case Airport.lineRE(iata, airport, city, state, country, lat, lng) =>
          Some(Row(iata.trim, airport.trim, city.trim, state.trim, country.trim, lat.toFloat, lng.toFloat))
        case line =>
          Console.err.println(s"ERROR: Invalid Airport line: $line")
          None
      }
      val airportsRDD = for {
        line <- sc.textFile(argz("airports"))
        airport <- parse(line)
      } yield airport

      val airports = sqlContext.createDataFrame(airportsRDD, schema)
      airports.cache

      out.println(s"# airports = ${airports.count}")

      // SELECT * FROM airports a WHERE a.state = "CA";
      val ca_airports = airports.filter(airports("state") === "CA")
      Printer(out, "California airports", ca_airports)

      // SELECT COUNT(*) FROM airports a WHERE a.country <> "USA";
      val nonus_airports = airports.filter(airports("country") !== "USA")
      Printer(out, "Non-US airports", nonus_airports)
    } finally {
      sc.stop()
    }
  }
}
