import com.typesafe.training.data._
import com.typesafe.training.util.Printer
import com.typesafe.training.util.sql.SparkSQLRDDUtil
// Our settings for sbt console and spark-shell both define the following for us:
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
// import org.apache.spark.sql.SQLContext
// val sqlContext = new SQLContext(sc)
// import sqlContext.implicits._  // Needed for column idioms like $"foo".desc.

// Change to a more reasonable default number of partitions for our data
// (from 200)
sqlContext.setConf("spark.sql.shuffle.partitions", "4")

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
// Switch to descending sort:
flights.orderBy($"origin".desc).show
// Two or more columns:
flights.orderBy(flights("origin"), flights("dest")).show

// The $"count".desc syntax appears to be the only (?) way to specify
// descending order.
// The $"..." is not a Scala feature, but Scala allows you to implement
// "interpolated string" handlers with your own prefix, `$` in this case.

// SELECT cf.date.month AS month, COUNT(*)
//   FROM canceled_flights cf
//   GROUP BY cf.date.month
val canceled_flights_by_month = canceled_flights.
  groupBy("date.month").count()
Printer(Console.out, "canceled flights by month", canceled_flights_by_month)

println("\ncanceled_flights_by_month.explain(true):")
canceled_flights_by_month.explain(true)

canceled_flights.unpersist

// Watch what happens with the next calculation.
// Before running the next query, change the shuffle.partitions property to 50:
sqlContext.setConf("spark.sql.shuffle.partitions", "50")

// We used "50" instead of "4". Run the query. How much time does it take?
//
// SELECT origin, dest, COUNT(*) AS cnt
//   FROM flights
//   GROUP BY origin, dest
//   ORDER BY cnt DESC, origin, dest;
val flights_between_airports50 = flights.select($"origin", $"dest").
  groupBy($"origin", $"dest").count().
  orderBy($"count".desc, $"origin", $"dest")
Printer(Console.out, "Flights between airports, sorted by airports", flights_between_airports50)

// Now change it back, run the query and compare the time. Does the output change?
sqlContext.setConf("spark.sql.shuffle.partitions", "4")
val flights_between_airports = flights.select($"origin", $"dest").
  groupBy($"origin", $"dest").count().
  orderBy($"count".desc, $"origin", $"dest")
Printer(Console.out, "Flights between airports, sorted by airports", flights_between_airports)

println("\nflights_between_airports.explain(true):")
flights_between_airports.explain(true)

flights_between_airports.cache

// Now note it's sometimes useful to coalesce to a smaller number of partitions
// after calling an operation like `groupBy`, where the number of resulting
// records may drop dramatically (but they records become correspondingly bigger!).
// Specifically, `groupBy` returns a `GroupedData` object, on which we call
// `count`, which returns a new `DataFrame`. That's what we coalesce on so that
// `orderBy` can potentially be much faster. HOWEVER, because we set the default
// number of partitions to 4 with the property above, in this particular case,
// this doesn't make much difference.
val flights_between_airports2 = flights.
  groupBy($"origin", $"dest").count().coalesce(2).
  sort($"count".desc, $"origin", $"dest")
Printer(Console.out, "Flights between airports, sorted by airports", flights_between_airports2)

println("\nflights_between_airports2.explain(true):")
flights_between_airports2.explain(true)

// SELECT origin, dest, COUNT(*)
//   FROM flights_between_airports
//   ORDER BY count DESC;
val frequent_flights_between_airports =
  flights_between_airports.orderBy($"count".desc)
// Show all of them (~170)
Printer(Console.out, "Flights between airports, sorted by counts descending", frequent_flights_between_airports, 200)

println("\nfrequent_flights_between_airports.explain(true):")
frequent_flights_between_airports.explain(true)
