import com.typesafe.training.data._
import com.typesafe.training.util.Printer
import com.typesafe.training.util.sql.SparkSQLRDDUtil
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, DataFrame}

// The following assumes you have already executed the SparkDataFrames script
// in the environment.

// Use a nested query to join more than two tables:
// sql("""
//   SELECT fa.origin, fa.airport, fa.dest, b.airport, fa.c2
//   FROM (
//     SELECT f.origin, a.airport, f.dest, f.c2
//     FROM flights_between_airports2 f
//     JOIN airports a ON f.origin = a.iata) fa
//   JOIN airports b ON fa.dest = b.iata""")

val fba = flights_between_airports
val air = airports
val fba2 = fba.
  join(air, fba("origin") === air("iata")).
    select("origin", "airport", "dest").
      toDF("origin", "oairport", "dest").
  join(air, $"dest"   === air("iata")).
    select("origin", "oairport", "dest", "airport").
      toDF("origin", "oairport", "dest", "dairport")

fba2.printSchema
fba2.show

// sql("""
//   SELECT *
//   FROM flights f
//   JOIN planes p ON f.tailNum = p.tailNum
//   LIMIT 100""")

val planes100 = flights.
  join(planes, flights("tailNum") === planes("tailNum")).
  limit(100)

planes100.printSchema
planes100.show



