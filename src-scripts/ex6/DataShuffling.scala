import com.typesafe.training.data.Airport
import org.apache.spark.{HashPartitioner, RangePartitioner, SparkContext}
import org.apache.spark.rdd.RDD

// Script version of com.typesafe.training.sws.ex6.DataShuffling.

val airportsFile = "data/airline-flights/airports.csv"
val numIterations = 5
val sleepInterval = 5000   // milliseconds
val airportPrefix = "S"

val airports = for {
  line <- sc.textFile(airportsFile)
  airport <- Airport.parse(line)
} yield airport.iata -> airport.airport
// Play with partitioning, with different # and with RangePartitioner:
// airports.partitionBy(new HashPartitioner(partitions = 64))
airports.cache

// Just the IATA
val airportCodesSample = airports.map(tup => tup._1).takeSample(true, 1000)

// Generate random flight data (origin_airport, (dest_airport, flight_data))
// out of the given array of airport codes.
def generateFlightLog(codes: Array[String]): RDD[(String, (String, String))] = {
  import scala.util.Random._
  // Try replacing "50" with other upper bounds:
  val log = sc.parallelize((1 to 50) map { i =>
    val origin = codes(nextInt(codes.length))
    val dest = codes(nextInt(codes.length))
    (origin, (dest, s"Random flight data $i"))
  })
  // Play with partitioning, with different # and with RangePartitioner:
  // log.partitionBy(new HashPartitioner(partitions = 64))
  log
}

def processFlightEveryFiveSeconds(callback: (RDD[(String, (String, String))]) => Unit): Unit = {
  (1 to numIterations) foreach { _ =>
    // Use a sample of the airport codes to generate an RDD with random pairs of codes.
    val log = generateFlightLog(airportCodesSample)
    callback(log)
    Thread.sleep(sleepInterval) // Poor man's way to delay work.
  }
}

def processFlightLog(airports: RDD[(String, String)]): Unit = {
  processFlightEveryFiveSeconds { flightLog =>
    val joined = airports.join(flightLog)
    val c = joined.filter {
      case (origin, (airport, (dest, flightData))) => dest.startsWith(airportPrefix)
    }.count()

    println(s"Total count of flights leaving for airports starting with '$airportPrefix' = " + c)
  }
}

processFlightLog(airports)
