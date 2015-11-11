package com.typesafe.training.sws.ex2

import com.typesafe.training.util.Timestamp   // Simple date-time utility
import org.apache.spark.SparkContext
// Implicit conversions, such as methods defined in
// org.apache.spark.rdd.PairRDDFunctions
// (http://spark.apache.org/docs/latest/api/core/index.html#org.apache.spark.rdd.PairRDDFunctions)
import org.apache.spark.SparkContext._

/**
 * Second implementation of Word Count.
 * Scala makes the Singleton Design Pattern "first class". The "object" keyword
 * declares an class with a single instance that the runtime will create itself.
 * You put definitions in objects that would be declared static in Java, like
 * "main".
 */
object WordCountB {
  def main(args: Array[String]): Unit = {

    // The first argument specifies the "master" (see the tutorial notes).
    // The second argument is a name for the job. Additional arguments
    // are optional.
    val sc = new SparkContext("local", "Word Count (B)")

    // Put the "stop" inside a finally clause, so it's invoked even when
    // something fails that forces an abnormal termination.
    try {
      // Load Shakespeare's plays creating an RDD.
      val input = sc.textFile("data/all-shakespeare.txt")

      // Convert to lower case and split on non-alphanumeric sequences of
      // characters. Since each single line is converted to a sequence of
      // words, we use flatMap to flatten the sequence of sequences into a
      // single sequence of words.
      // Finally, countByValue finds all the same words and counts them.
      // It doesn't return an RDD, but a Scala Map.
      val wc = input
        .map(line => line.toLowerCase)
        .flatMap(line => line.split("""\W+"""))
        .map(word => (word, 1))
        .reduceByKey((n1,n2) => n1+n2)

      // Save to a file, but because we no longer have an RDD, we have to use
      // good 'ol Java File IO. Note that the output specifier is now a file,
      // not a directory as before, the format of each line will be different,
      // and the order of the output will not be the same, either.
      val now = Timestamp.now()
      val outpath = s"output/shakespeare-wcB-$now"
      println(s"Writing ${wc.count} records to $outpath")

      val out = new java.io.PrintWriter(outpath)
      wc.collect().foreach {
//        tuple => out.println("%20s\t%d".format(tuple._1, tuple._2))
        case (word, count) => out.println("%20s\t%d".format(word, count))
      }
      // WARNING: Without this close statement, it appears the output stream is
      // not completely flushed to disk!
      out.close()
    } finally {
      sc.stop()      // Stop (shut down) the context.
    }
  }
}
