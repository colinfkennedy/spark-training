package com.typesafe.training.sws.ex2.solns

import com.typesafe.training.util.{Timestamp, CommandLineOptions}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Sort by word length. #5 in the list for Exercise 2.
 */
object WordCountSortByWordLength {
  def main(args: Array[String]): Unit = {

    val options = CommandLineOptions(
      this, "",
      CommandLineOptions.inputPath(Some("data/all-shakespeare.txt")),
      CommandLineOptions.outputPath(Some("output/shakespeare-wc-sort-by-word-length")),
      CommandLineOptions.master(Some(CommandLineOptions.defaultMaster)),
      CommandLineOptions.quiet)
    val argz = options(args.toList)

    val sc = new SparkContext(argz("master"), "Word Count - Sort by Word Length")

    try {
      val input = sc.textFile(argz("input-path")).map(_.toLowerCase)
      // Sacred Texts: If you try this with one of the "sacred texts", uncomment:
      //  .map(_.split("""\s*\|\s*""").last)

      // After reduceByKey, insert the word length and sort by it.
      val wc = input
        .flatMap(line => line.split("""\W+"""))
        .map(word => (word, 1))
        .reduceByKey((n1, n2) => n1 + n2)
        // Construct new records that add the word length.
        // Use the length as the "key" for sorting.
        .map{ case (word, count) => (word.length, word, count) }
        // Alternative to two previous lines: if you don't care about
        // the actual length, don't add it to the tuple. Instead, just
        // use "keyBy" or "sortBy" on the word length:
        // .keyBy(tuple => tuple._1.length)
        // Return negative length for descending sort.
        .sortBy{ case (length, _, _) => -length }

      val now = Timestamp.now()
      val out = s"${argz("output-path")}-$now"
      println(s"Writing output to: $out")
      wc.saveAsTextFile(out)
    } finally {
      sc.stop()
    }
  }
}
