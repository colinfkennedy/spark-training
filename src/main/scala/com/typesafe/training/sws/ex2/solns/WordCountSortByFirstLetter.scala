package com.typesafe.training.sws.ex2.solns

import com.typesafe.training.util.{Timestamp, CommandLineOptions}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Sort by word. #3 in the list for Exercise 2.
 */
object WordCountSortByFirstLetter {
  def main(args: Array[String]): Unit = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName, "",
      CommandLineOptions.inputPath(Some("data/all-shakespeare.txt")),
      CommandLineOptions.outputPath(Some("output/shakespeare-wc-sort-by-count-first-letter")),
      CommandLineOptions.master(Some("local")),
      CommandLineOptions.quiet)
    val argz = options(args.toList)

    val sc = new SparkContext(argz("master"), "Word Count - Sort by First Letter")

    try {
      val input = sc.textFile(argz("input-path")).map(_.toLowerCase)
      // Sacred Texts: If you try this with one of the "sacred texts", uncomment:
      //  .map(_.split("""\s*\|\s*""").last)

      // After reduceByKey, simply sortByKey, which uses the first element
      // in the tuple, the word.
      val wc = input
        .flatMap(line => line.split("""\W+"""))
        // The next line is new, followed by a trivial change to the "map"
        // step in the previous exercise; we keep only the first letter of
        // the word. The added line handles the case where the we have empty
        // words in the stream!
        .filter(word => word.length > 0)
        .map(word => (word(0), 1))
        .reduceByKey((n1, n2) => n1 + n2)
        // Pass true for ascending, false for descending.
        .sortByKey(false)

      val now = Timestamp.now()
      val out = s"${argz("output-path")}-$now"
      println(s"Writing output to: $out")
      wc.saveAsTextFile(out)
    } finally {
      sc.stop()
    }
  }
}
