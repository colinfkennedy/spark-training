package com.typesafe.training.sws.ex2.solns

import com.typesafe.training.util.{Timestamp, CommandLineOptions}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Sort by word count. #4 in the list for Exercise 2.
 */
object WordCountSortByCount {
  def main(args: Array[String]): Unit = {

    val options = CommandLineOptions(
      this, "",
      CommandLineOptions.inputPath(Some("data/all-shakespeare.txt")),
      CommandLineOptions.outputPath(Some("output/shakespeare-wc-sort-by-count")),
      CommandLineOptions.master(Some(CommandLineOptions.defaultMaster)),
      CommandLineOptions.quiet)
    val argz = options(args.toList)

    val sc = new SparkContext(argz("master"), "Word Count - Sort by Word Count")

    try {
      val input = sc.textFile(argz("input-path")).map(_.toLowerCase)
      // Sacred Texts: If you try this with one of the "sacred texts", uncomment:
      //  .map(_.split("""\s*\|\s*""").last)

      // After calling reduceByKey, use the second element of the tuple,
      // the count, as the key, then sort by the key descending (false) or try
      // ascending (true).
      val wc = input
        .flatMap(line => line.split("""\W+"""))
        .map(word => (word, 1))
        .reduceByKey((n1, n2) => n1 + n2)
        .keyBy(tuple => tuple._2)  // Create a new RDD with the count as the key!
        // Add this line to sort: pass true for ascending, false for descending.
        .sortByKey(false)
        // At this point, the "schema" is (count, (word, count))
        // Let's convert it back to (word, count). You could do this:
        // .map { case (_ (word, count)) => (word, count) }
        // While pretty, the following is probably more efficient:
        .map(record => record._2)

      val now = Timestamp.now()
      val out = s"${argz("output-path")}-$now"
      println(s"Writing output to: $out")
      wc.saveAsTextFile(out)
    } finally {
      sc.stop()
    }
  }
}
