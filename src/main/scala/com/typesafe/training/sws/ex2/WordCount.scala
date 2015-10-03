package com.typesafe.training.sws.ex2

import com.typesafe.training.util.{Timestamp, CommandLineOptions}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Final implementation of Word Count that makes the following changes:
 * <ol>
 * <li>A simpler approach is used for the algorithm.</li>
 * <li>A CommandLineOptions library is used.</li>
 * <li>The handling of the per-line data format is refined.</li>
 * </ol>
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    // I extracted command-line processing code into a separate utility class,
    // an illustration of how it's convenient that we can mix "normal" code
    // with "big data" processing code.
    val options = CommandLineOptions(
      this, "",
      CommandLineOptions.inputPath(Some("data/all-shakespeare.txt")),
      CommandLineOptions.outputPath(Some("output/shakespeare-wc")),
      CommandLineOptions.master(Some(CommandLineOptions.defaultMaster)),
      CommandLineOptions.quiet)
    val argz = options(args.toList)

    val sc = new SparkContext(argz("master"), "Word Count")

    try {
      // Load the input text, convert each line to lower case.
      // The output is an RDD.
      val input = sc.textFile(argz("input-path")).map(_.toLowerCase)
      // Sacred Texts: If you try this with one of the "sacred texts",
      // they have this format:
      //   book|chapter|verse|text
      // You'll want to keep only the text, so uncomment the following line,
      // which splits the line on the delimiter and keeps only the last element
      // in the array, which is the text.
      //  .map(_.split("""\s*\|\s*""").last)

      // Split on non-alphanumeric sequences of character as before.
      // Then, map the words into tuples that add a count of 1 for each word.
      // Finally, reduceByKey functions like a SQL "GROUP BY" followed by
      // a count of the elements in each group. The words are the keys and
      // values are the 1s, which are added together, effectively counting
      // the occurrences of each word.
      val wc = input
        .flatMap(text => text.split("""\W+"""))  // or .flatMap(_.split("""\W+"""))
        .map(word => (word, 1))  // RDD[(String,Int)]
        .reduceByKey((n1, n2) => n1 + n2)   // or .reduceByKey(_ + _)

      // Save, but it actually writes Hadoop-style output; to a directory,
      // with a _SUCCESS marker (empty) file, the data as a "part" file,
      // and checksum files.
      // We append a timestamp, because Spark, like Hadoop, won't let us
      // overwrite an existing directory, e.g., from a prior run.
      val now = Timestamp.now()
      val outpath = s"${argz("output-path")}-$now"
      println(s"Writing output to: $outpath")
      wc.saveAsTextFile(outpath)
    } finally {
      // Console.in.read()
      sc.stop()
    }
  }
}
