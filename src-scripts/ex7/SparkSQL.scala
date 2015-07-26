// Adapted from src/main/scala/com/typesafe/training/sws/ex7/SparkSQL.scala
import com.typesafe.training.data._
import com.typesafe.training.util.Printer
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.rdd.RDD
import com.typesafe.training.util.sql.SparkSQLRDDUtil

val flightsPath  = "data/airline-flights/alaska-airlines/2008.csv"
val carriersPath = "data/airline-flights/carriers.csv"
val airportsPath = "data/airline-flights/airports.csv"
val planesPath   = "data/airline-flights/plane-data.csv"

// Change to a more reasonable default number of partitions (from 200)
sqlContext.setConf("spark.sql.shuffle.partitions", "4")

// Our settings for sbt console and spark-shell both define the following for us:
// val sqlContext = new SQLContext(sc)

import sqlContext.sql

def print(message: String, df: DataFrame) = {
  println(message)
  df.show()
}

val (flights, carriers, airports, planes) =
  SparkSQLRDDUtil.load(sc, flightsPath, carriersPath, airportsPath, planesPath)
// Cache just the flights and airports.
flights.cache
airports.cache

// Register the DataFrames as temporary "tables".
// The `[T <: Product : TypeTag]` means that the type T must be a
// subtype of the trait `Product` (inherited by Tuples, for example; it provides
// the `foo._2` methods, etc.). Also, T must be *convertable* to a TypeTag[T]
// using an implicit conversion. TypeTags are part of the reflection API and are
// used to determine certain type information at runtime.
import scala.reflect.runtime.universe.TypeTag
def register[T <: Product : TypeTag](rdd: RDD[T], name: String): Unit = {
  val df = sqlContext.createDataFrame(rdd)
  df.registerTempTable(name)
  df.cache()
  println(s"Schema for $name:")
  df.printSchema()
}

register(flights,  "flights")
register(carriers, "carriers")
register(airports, "airports")
register(planes,   "planes")

// Write some queries!!

val total_flights = flights.count
println(s"total_flights = ${flights.count}")

val canceled_flights = sql(
  "SELECT COUNT(*) FROM flights f WHERE f.canceled > 0")
print("canceled flights", canceled_flights)
println("\ncanceled_flights.explain(extended = false):")
canceled_flights.explain(extended = false)
println("\ncanceled_flights.explain(extended = true):")
canceled_flights.explain(extended = true)
println("The query plan:")
canceled_flights.queryExecution

// NOTE: If we registered canceled_flights as a table, we could use it
// and eliminate the WHERE clause.
val canceled_flights_by_month = sql("""
  SELECT f.date.month AS month, COUNT(*)
  FROM flights f
  WHERE f.canceled > 0
  GROUP BY f.date.month
  ORDER BY month""")
print("canceled flights by month", canceled_flights_by_month)
println("\ncanceled_flights_by_month.explain(true):")
canceled_flights_by_month.explain(true)

val flights_between_airports = sql("""
  SELECT origin, dest, COUNT(*)
  FROM flights
  GROUP BY origin, dest
  ORDER BY origin, dest""")
print("Flights between airports, sorted by airports", flights_between_airports)
println("\nflights_between_airports.explain(true):")
flights_between_airports.explain(true)

val flights_between_airports2 = sql("""
  SELECT origin, dest, COUNT(*) AS cnt
  FROM flights
  GROUP BY origin, dest
  ORDER BY cnt DESC""")
print("Flights between airports, sorted by counts", flights_between_airports2)
println("\nflights_between_airports2.explain(true):")
flights_between_airports2.explain(true)

// Register this table so you can play with it later.
flights_between_airports2.registerTempTable("flights_between_airports2")
print("Flights between airports #2, sorted by count", flights_between_airports2)

// Write your own queries. Try joins with the other "tables".
// Register canceled_flights as a table and rewrite canceled_flights_by_month's
// query to use it.
