package com.typesafe.training.sws.ex2.solns

import com.typesafe.training.util.{Timestamp, CommandLineOptions}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Group over the word counts. #6 in the list for Exercise 2.
 */
object WordCountGroupByWordCounts {
  def main(args: Array[String]): Unit = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName, "",
      CommandLineOptions.inputPath(Some("data/all-shakespeare.txt")),
      CommandLineOptions.outputPath(Some("output/shakespeare-wc-sort-by-word-count-groups")),
      CommandLineOptions.master(Some("local")),
      CommandLineOptions.quiet)
    val argz = options(args.toList)

    val sc = new SparkContext(argz("master"), "Group By Word Counts")

    try {
      val input = sc.textFile(argz("input-path")).map(_.toLowerCase)
      // Sacred Texts: If you try this with one of the "sacred texts", uncomment:
      //  .map(_.split("""\s*\|\s*""").last)

      // After calling reduceByKey, groupBy the word counts, the second element
      // in the tuples. Then sort ascending (true) or descending (false).
      val wc = input
        .flatMap(line => line.split("""\W+"""))
        .map(word => (word, 1))
        .reduceByKey((n1, n2) => n1 + n2)
        .keyBy(tuple => tuple._2)   // Use the 2nd tuple element as the key
        .groupByKey
        // Could also write the previous in one step as follows (but might be slightly
        // less efficient):
        // .groupBy(tuple => tuple._2)
        .sortByKey(true)              // also try descending (false)
        // The following sorting of each group's words is not necessary for the
        // exercise, but it means the results will be consistent for unit tests!
        // Note that we convert the iterable to a Vector, which has O(1)
        // random access! While we're at it, we'll also eliminate the redundant
        // count2 values
        .map { case (count, iter) =>
          (count, iter.toVector.map(tup => tup._1).sortBy(word => word))
        }

      val now = Timestamp.now()
      val out = s"${argz("output-path")}-$now"
      println(s"Writing output to: $out")
      wc.saveAsTextFile(out)
    } finally {
      sc.stop()
    }
  }
}
