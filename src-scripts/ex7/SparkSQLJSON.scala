import com.typesafe.training.data._
import com.typesafe.training.util.Printer
import org.apache.spark.sql.{SQLContext, DataFrame}

// Change to a more reasonable default number of partitions (from 200)
sqlContext.setConf("spark.sql.shuffle.partitions", "4")

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
val carriersRDD = carriersDF.toJSON
carriersRDD.take(5).foreach(println)
Printer(Console.out, "Carriers converted to an RDD of JSON strings: ", carriersRDD)
println("\nCarriers whose 'code' starts with 'U':")
carriersRDD.filter(str => str.contains("""code":"U""")) foreach println

println("\nError handling. Note how the following bad records are handled.")
val jsonsRDD1 = sc.parallelize(Seq(
  """{ "id": 1, "message": "This is a good record" }""",
  """{ "id", "message": "This is not a good record" }""",
  """{ "id": 3, "message" }""",
  """{ "id": 4, "message": "This is another good record" }"""))
val jsons1 = sqlContext.read.json(jsonsRDD1).cache
jsons1.schema
jsons1.show
val good = jsons1.filter($"_corrupt_record".isNull)
val bad  = jsons1.filter($"_corrupt_record".isNotNull)
println(s"${good.count} good records:")
good foreach println
println(s"${bad.count} bad records:")
bad foreach println

println("\nWhat happens if you have missing key-value pairs? You get the superset schema.")
val jsonsRDD2 = sc.parallelize(Seq(
  """{ "id": 1, "message": "message1", "name": "name1" }""",
  """{ "id": 2, "message": "message2"                  }""",
  """{ "id": 3,                        "name": "name3" }""",
  """{          "message": "message4", "name": "name4" }"""))
val jsons2 = sqlContext.read.json(jsonsRDD2).cache
jsons2.schema
jsons2.show

println("\nIn the previous example, id was of type 'bigint'. Now what happens to it? What is the type for 'number'?")
val jsonsRDD3 = sc.parallelize(Seq(
  """{ "id": 1,     "message": "message1", "number": 1 }""",
  """{ "id": "two", "message": "message1", "number": 2.2 }"""))
val jsons3 = sqlContext.read.json(jsonsRDD3).cache
jsons3.schema
jsons3.show

println("\nIs nesting handled correctly? YES!")
val jsonsRDD4 = sc.parallelize(Seq(
  """{ "id": 1, "level1": { "a": "a1", "b": "b1", "level2": { "c": "c1" } } }""",
  """{ "id": 2, "level1": { "a": "a2", "b": "b2", "level2": { "c": "c2" } } }"""))
val jsons4 = sqlContext.read.json(jsonsRDD4).cache
jsons4.schema
jsons4.show
println("\nLet's select the fields:")
println("id, level1")
jsons4.select($"id", $"level1").show
println("id, level1.a")
jsons4.select($"id", $"level1.a").show
println("id, level1.b")
jsons4.select($"id", $"level1.b").show
println("id, level1.level2")
jsons4.select($"id", $"level1.level2").show
println("id, level1.level2.c")
jsons4.select($"id", $"level1.level2.c").show

println("\nRevisiting mixed types. What's the type of 'level2'?")
val jsonsRDD5 = sc.parallelize(Seq(
  """{ "id": 1, "level1": { "a": "a1", "b": "b1", "level2": { "c": "c1" } } }""",
  """{ "id": 2, "level1": { "a": "a2", "b": "b2", "level2": 2.2 } }"""))
val jsons5 = sqlContext.read.json(jsonsRDD5).cache
jsons5.schema
jsons5.show

