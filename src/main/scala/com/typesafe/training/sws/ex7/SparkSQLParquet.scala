package com.typesafe.training.sws.ex7

import com.typesafe.training.util.CommandLineOptions
import com.typesafe.training.util.CommandLineOptions.Opt
import com.typesafe.training.util.Printer
import com.typesafe.training.data.{Verse, Abbrev}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

/**
 * Use SparkSQL to read and write Parquet files.
 * Writes "query" results to the console, rather than a file.
 * Also uses the King James Version of the Bible.
 * NOTE: You'll see errors or warnings (depending on how you run this)
 * that it can't load org.slf4j.impl.StaticLoggerBinder and it can't
 * initialize a counter because something or other isn't a "TaskInputOutputContext".
 * This is typical jar Hell with Hadoop software, when you attempt to
 * use it outside a fully-configured Hadoop environment. You can safely
 * ignore these messages.
 */
object SparkSQLParquet {

  var out = Console.out // overload in test double
  var quiet = false

  def main(args: Array[String]): Unit = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName, "",
      CommandLineOptions.master(Some("local[2]")),
      CommandLineOptions.inputPath(Some("data/kjvdat.txt")),
      CommandLineOptions.outputPath(Some("output/verses.parquet")),
      CommandLineOptions.quiet)
    val argz = options(args.toList)
    quiet = argz.getOrElse("quiet", "false").toBoolean

    val sc = new SparkContext(argz("master"), "Spark SQL Parquet")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._  // Needed for column idioms like $"foo".desc.

    try {

      // Load the data and write to Parquet files.
      val versesRDD = for {
        line  <- sc.textFile(argz("input-path"))
        verse <- Verse.parse(line) // If None is returned, this line discards it.
      } yield verse

      val verses = sqlContext.createDataFrame(versesRDD)

      out.println("Select verses that mention Babylon:")
      val babylon = verses.filter($"text".contains("Babylon"))
      Printer(out, s"Verses that mention Babylon.", babylon)

      val outPath = argz("output-path")
      out.println(s"Saving 'verses' as a Parquet file to $outPath.")
      val parquetDir = new java.io.File(outPath)
      if (parquetDir.exists) {
        println(s"Deleting old $outPath")  // DON'T write to out; messes up unit tests.
        parquetDir.listFiles foreach (_.delete)
        parquetDir.delete
      }

      // save() uses Parquet.
      verses.write.save(outPath)

      // Now read it back in and use it:
      out.println(s"Reading in the Parquet file from $outPath:")
      val verses2 = sqlContext.read.parquet(outPath)
      if (!quiet) {
        verses2.printSchema
        verses2.show
      }

      out.println("Using the DataFrame loaded from the Parquet File, select Babylon verses:")
      val babylon2 = verses2.filter($"text".contains("Babylon"))
      Printer(out, s"Verses that mention Babylon.", babylon2)

    } finally {
      sc.stop()
    }
  }

  private def print[T](msg: String, rdd: RDD[T], n: Int = 100): Unit = {
    out.println(s"$msg: (size = ${rdd.count})")
    rdd.take(n) foreach out.println
  }
}
