import com.typesafe.training.data.{Verse, Abbrev}
import com.typesafe.training.util.Printer
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

// Change to a more reasonable default number of partitions (from 200)
sqlContext.setConf("spark.sql.shuffle.partitions", "4")

// Our settings for sbt console and spark-shell both define the following for us:
// val sqlContext = new SQLContext(sc)
// import sqlContext.implicits._  // Needed for column idioms like $"foo".desc.

// Use SparkSQL to read and write Parquet files.
// Writes "query" results to the console, rather than a file.
// Also uses the King James Version of the Bible.

val inputPath  = "data/kjvdat.txt"
val outputPath = "output/verses.parquet"

// Load the data and write to Parquet files.
val versesRDD = for {
  line  <- sc.textFile(inputPath)
  verse <- Verse.parse(line) // If None is returned, this line discards it.
} yield verse
val verses = sqlContext.createDataFrame(versesRDD)
verses.printSchema
verses.show
println("Select verses that mention Babylon:")
val babylon = verses.filter($"text".contains("Babylon"))
Printer(Console.out, s"Verses that mention Babylon.", babylon)

println(s"Saving 'verses' as a Parquet file to $outputPath.")
val parquetDir = new java.io.File(outputPath)
if (parquetDir.exists) {
  println(s"Deleting old $outputPath")
  parquetDir.listFiles foreach (_.delete)
  parquetDir.delete
}

// You can also call verses.write.save(), which uses Parquet.
verses.write.parquet(outputPath)

// Now read it back in and use it:
println(s"Reading in the Parquet file from $outputPath:")
val verses2 = sqlContext.read.parquet(outputPath)
verses2.printSchema
verses2.show

println("Using the DataFrame loaded from the Parquet File, select Babylon verses:")
val babylon2 = verses2.filter($"text".contains("Babylon"))
Printer(Console.out, s"Verses that mention Babylon.", babylon2)
