import com.typesafe.training.data._
import com.typesafe.training.util.Printer
import org.apache.spark.sql.{SQLContext, DataFrame}

/** Example of loading JSON */

val carriersJSONPath = "data/airline-flights/carriers.json"
val carriersCSVPath = "data/airline-flights/carriers.csv"
val output = "output/carriers-json"

// Load from JSON this time, creating a DataFrame:
val carriersJSON = sqlContext.read.json(carriersJSONPath)
carriersJSON.printSchema()
Printer(Console.out, "Carriers loaded using jsonFile: ", carriersJSON)
println("\nCarriers whose 'code' starts with 'U':")
carriersJSON.filter($"code".startsWith("U")).show

println(s"\nWriting as JSON to $output")
carriersJSON.write.json(output)

// Also load a raw strings
val carriersJSONStrings = for {
  line <- sc.textFile(carriersJSONPath)
} yield line

// Infer the schema using the raw strings:
val carriersJSON2 = sqlContext.read.json(carriersJSONStrings)
carriersJSON2.printSchema()
Printer(Console.out, "Carriers loaded using jsonRDD: ", carriersJSON2)
println("\nCarriers whose 'code' starts with 'U':")
carriersJSON2.filter($"code".startsWith("U")).show

// Read in CVS, convert to a dataframe, then convert to JSON.
val carriersCSV = for {
  line <- sc.textFile(carriersCSVPath)
  carrier <- Carrier.parse(line)
} yield carrier
val carriersDF = sqlContext.createDataFrame(carriersCSV)
carriersDF.printSchema()
val carriersJSON3 = carriersDF.toJSON
carriersJSON3.take(5).foreach(println)
Printer(Console.out, "Carriers converted to JSON: ", carriersJSON3)
println("\nCarriers whose 'code' starts with 'U':")
carriersJSON3.filter($"code".startsWith("U")).show

// Error handling. Note how the following bad records are handled.
val jsonsRDD = sc.parallelize(Seq(
  """{ "id": 1, "message": "This is a good record" }""",
  """{ "id", "message": "This is not a good record" }""",
  """{ "id": 3, "message" }""",
  """{ "id": 4, "message": "This is another good record" }"""))
val jsons = sqlContext.read.json(jsonsRDD).cache
jsons.schema
jsons.show
val good = jsons.filter($"_corrupt_record".isNull)
val bad  = jsons.filter($"_corrupt_record".isNotNull)
println(s"${good.count} good records:")
good foreach println
println(s"${bad.count} bad records:")
bad foreach println
