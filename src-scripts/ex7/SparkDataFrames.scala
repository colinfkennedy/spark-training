import com.typesafe.training.data._
import com.typesafe.training.util.Printer
import com.typesafe.training.util.sql.SparkSQLRDDUtil
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, DataFrame}

// Our settings for sbt console and spark-shell both define the following for us:
// val sqlContext = new SQLContext(sc)
// import sqlContext.implicits._  // Needed for column idioms like $"foo".desc.

val flightsPath  = "data/airline-flights/alaska-airlines/2008.csv"
val carriersPath = "data/airline-flights/carriers.csv"
val airportsPath = "data/airline-flights/airports.csv"
val planesPath   = "data/airline-flights/plane-data.csv"

// Use pattern matching tricks to extract the elements from the tuple
// of RDDs returned by SparkSQLRDDUtil, and then from a Seq used to
// iterate over the four RDDs to convert to DataFrames:
val (flightsRDD, carriersRDD, airportsRDD, planesRDD) =
  SparkSQLRDDUtil.load(sc, flightsPath, carriersPath, airportsPath, planesPath)
val flights  = sqlContext.createDataFrame(flightsRDD)
val carriers = sqlContext.createDataFrame(carriersRDD)
val airports = sqlContext.createDataFrame(airportsRDD)
val planes   = sqlContext.createDataFrame(planesRDD)

Seq(flights, carriers, airports, planes) foreach { df =>
  df.printSchema
  df.show()  // show() prints 20 records in the DataFrame.
}

// Cache just the flights and airports.
flights.cache
airports.cache

// If you want to rename the columns, you can create a new DataFrame
// with toDF. There is also an overload of `SQLContext#createDataFrame`
// that lets you do this from an RDD.
carriers.printSchema
val c2 = carriers.toDF("abbreviation", "name")
c2.printSchema


// Write some "queries" using the DataFrame API.
// We'll show analogous SQL queries that we'll use in the next exercise,
// pretending that we have "tables" with same names as the DataFrame
// variable names.

println(s"total_flights = ${flights.count}")

// SELECT COUNT(*) FROM flights f WHERE f.canceled > 0;
val canceled_flights = flights.filter(flights("canceled") > 0)
Printer(Console.out, "canceled flights", canceled_flights)

println("\ncanceled_flights.explain(extended = false):")
canceled_flights.explain(extended = false)
println("\ncanceled_flights.explain(extended = true):")
canceled_flights.explain(extended = true)

canceled_flights.cache

// Note how we can reference the columns several ways:
flights.orderBy(flights("origin")).show
flights.orderBy("origin").show
flights.orderBy($"origin").show
flights.orderBy($"origin".desc).show

// The last one $"count".desc is the only (?) way to specify descending order.
// The $"..." is not a Scala feature, but Scala allows you to implement
// "interpolated string" handlers with your own prefix ($ in this case).

// SELECT cf.date.month AS month, COUNT(*)
//   FROM canceled_flights cf
//   GROUP BY cf.date.month
//   ORDER BY month;
val canceled_flights_by_month = canceled_flights.
  groupBy("date.month").count()
Printer(Console.out, "canceled flights by month", canceled_flights_by_month)

println("\ncanceled_flights_by_month.explain(true):")
canceled_flights_by_month.explain(true)

canceled_flights.unpersist

// Watch what happens with the next calculation.
// Note how many partitions it generates and how long it takes to run.
// SELECT origin, dest, COUNT(*)
//   FROM flights
//   GROUP BY origin, dest
//   ORDER BY origin, dest;
val flights_between_airports1 = flights.
  groupBy("origin", "dest").count().
  orderBy("origin", "dest")
Printer(Console.out, "Flights between airports, sorted by airports", flights_between_airports1)

println("\nflights_between_airports1.explain(true):")
flights_between_airports1.explain(true)

// Now note what happens if we coalesce to 4 partitions after calling
// `groupBy`, which expands the number of partitions. Specifically,
// `groupBy` returns a `GroupedData` object, on which we call `count`,
// which returns a new `DataFrame`. That's what we coalesce on so that
// `orderBy` is much faster.
val flights_between_airports = flights.
  groupBy("origin", "dest").count().coalesce(4).
  orderBy("origin", "dest")
Printer(Console.out, "Flights between airports, sorted by airports", flights_between_airports)

println("\nflights_between_airports.explain(true):")
flights_between_airports.explain(true)

flights_between_airports.cache

// The call to coalesce(4) seems redundant, but apparently isn't.
// If you remove it, `orderBy` processes ~170 partitions.
// SELECT origin, dest, COUNT(*)
//   FROM flights_between_airports
//   ORDER BY count DESC;
val frequent_flights_between_airports = flights_between_airports.
  coalesce(4).orderBy($"count".desc)
// Show all of them (~170)
Printer(Console.out, "Flights between airports, sorted by counts descending", frequent_flights_between_airports, 200)

println("\nfrequent_flights_between_airports.explain(true):")
frequent_flights_between_airports.explain(true)
