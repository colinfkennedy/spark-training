package com.typesafe.training.sws.ex5

import com.typesafe.training.util.{Timestamp, CommandLineOptions}
import com.typesafe.training.util.CommandLineOptions.Opt
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/** NGrams - Find the ngrams in a corpus */
object NGrams {

  // Override for tests.
  var out = Console.out

  def main(args: Array[String]): Unit = {

    /** A function to generate an Opt for handling the count argument. */
    def count(value: Option[String]): Opt = Opt(
      name   = "count",
      value  = value,
      help   = s"-c | --count  N                   The number of NGrams to compute.",
      parser = {
        case ("-c" | "--count") +: n +: tail => (("count", n), tail)
      })

    /**
     * The NGram phrase to match, e.g., "I love % %" will find 4-grams that
     * start with "I love", and "% love %" will find trigrams with "love" as the
     * second word.
     * The "%" are replaced by the regex "\w+" and whitespace runs are replaced
     * with "\s+" to create a matcher regex.
     */
    def ngrams(value: Option[String]): Opt = Opt(
      name   = "ngrams",
      value  = value,
      help   = s"-n | --ngrams  S                  The NGrams match string.",
      parser = {
        case ("-n" | "--ngrams") +: s +: tail => (("ngrams", s), tail)
      })

    val options = CommandLineOptions(
      this.getClass.getSimpleName, "",
      CommandLineOptions.inputPath(Some("data/all-shakespeare.txt")),
      // CommandLineOptions.outputPath(Some("output/ngrams")), // just write to the console
      CommandLineOptions.master(Some("local")),
      CommandLineOptions.quiet,
      count(Some("100")),
      ngrams(Some("% love % %")))
    val argz = options(args.toList)

    val n = argz("count").toInt
    val ngramsStr = argz("ngrams").toLowerCase
    // Note that the replacement strings use Scala's triple quotes; necessary
    // to ensure that the final string is "[\w']+" (to handle words and
    // contractions of words) and "\s+" for the regexs.
    val ngramsRE = ngramsStr
      .replaceAll("%", """[\\w']+""").replaceAll("\\s+", """\\s+""").r

    val sc = new SparkContext(argz("master"), "NGrams")
    try {

      /**
       * Order the (ngram,count) pairs by count descending, ngram ascending.
       * We only do the latter so unit tests pass predictably!
       */
      object CountOrdering extends Ordering[(String, Int)] {
        def compare(a: (String, Int), b: (String, Int)): Int = {
          val cntdiff = a._2 compare b._2
          if (cntdiff != 0) -cntdiff else (a._1 compare b._1)
        }
      }

      // Load the input data. Note that NGrams across line boundaries are not
      // supported by this implementation.

      val ngramz = sc.textFile(argz("input-path"))
        .flatMap { line =>
            val text = line.toLowerCase
              // Once again, if you're using one of the "sacred texts",
              // uncomment this line:
              // .split("\\s*\\|\\s*").last
            ngramsRE.findAllMatchIn(text).map(_.toString)
        }
        .map(ngram => (ngram, 1))
        .reduceByKey(_ + _)
        .takeOrdered(n)(CountOrdering)
        // The following would work instead if sorting by ngrams, since
        // the ngram is in the "key position", but it would be very
        // inefficient to do a total ordering, then take the top n.
        // takeOrdered(...)(...) should be used in both cases:
        // .sortByKey(false)  // false for descending
        // .take(n)           // "LIMIT n"

      // Write to the console, but because we no longer have an RDD,
      // we have to use good 'ol Java File IO. Note that the output
      // specifier is now interpreted as a file, not a directory as before.
      out.println(s"Found ${ngramz.size} ngrams:")
      ngramz foreach {
        case (ngram, count) => out.println("%30s\t%d".format(ngram, count))
      }
    } finally {
      sc.stop()
    }
  }
}
