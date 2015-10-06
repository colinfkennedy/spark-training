import com.typesafe.training.data._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, FloatType}

/**
 * An example of using the Row idiom with input text data when constructing
 * a DataFrame. This is an alternative approach to using case classes, as in
 * `SparkDataFrames`.
 */

val airportsPath = "data/airline-flights/airports.csv"

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
  line <- sc.textFile(airportsPath)
  airport <- parse(line)
} yield airport

val airports = sqlContext.createDataFrame(airportsRDD, schema)
airports.cache

println(s"# airports = ${airports.count}")

// SELECT * FROM airports a WHERE a.state = "CA";
val ca_airports = airports.filter(airports("state") === "CA")
ca_airports.show()

// SELECT COUNT(*) FROM airports a WHERE a.country <> "USA";
val nonus_airports = airports.filter(airports("country") !== "USA")
nonus_airports.show()
