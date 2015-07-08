package com.typesafe.training.sws.ex4.solns

import com.typesafe.training.util.{Timestamp, CommandLineOptions, StopWords}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast

/**
 * Inverted Index - Basis of Search Engines.
 * Builds on the exercise solution that sorts by words and sorts the list of
 * (file_name, count) values by count descending. This version also filters
 * stop words, propagated as a Broadcast Variable.
 */
object InvertedIndexSortByWordsAndCountsWithStopWordsFiltering {
  def main(args: Array[String]): Unit = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName, "",
      CommandLineOptions.inputPath(Some("output/crawl")),
      CommandLineOptions.outputPath(Some("output/inverted-index-sorted-stop-words-removed")),
      CommandLineOptions.master(Some("local[*]")),
      CommandLineOptions.quiet)
    val argz = options(args.toList)

    val sc = new SparkContext(argz("master"), "Inverted Index - Sort by Word & Counts, Remove Stop Words")
    val stopWords: Broadcast[Set[String]] = sc.broadcast(StopWords.words)

    try {
      // Load the input "crawl" data, where each line has the format:
      //   (document_id, text)
      // First remove the outer parentheses, split on the first comma,
      // trim whitespace from the name (we'll do it later for the text)
      // and convert the text to lower case.
      // NOTE: The args("input-path") is a directory; Spark finds the correct
      // data files, part-NNNNN.
      val lineRE = """^\s*\(([^,]+),(.*)\)\s*$""".r
      val input = sc.textFile(argz("input-path")) map {
        case lineRE(name, text) => (name.trim, text.toLowerCase)
        case badLine =>
          Console.err.println(s"Unexpected line: $badLine")
          // If any of these were returned, you could filter them out below.
          ("", "")
      }

      val now = Timestamp.now()
      val out = s"${argz("output-path")}-$now"
      if (! argz.getOrElse("quiet", "false").toBoolean)
        println(s"Writing output to: $out")

      // Split on non-alphanumeric sequences of character as before.
      // Rather than map to "(word, 1)" tuples, we treat the words by values
      // and count the unique occurrences.
      input
        .flatMap {
          case (path, text) =>
            // If we don't trim leading whitespace, the regex split creates
            // an undesired leading "" word! Also, setup the (word,path) as
            // the key for reducing, and an initial count of 1.
            // Use a refined regex to retain abbreviations, e.g., "there's".
            text.trim.split("""[^\w']""") map (word => ((word, path), 1))
        }
        // New: Filter stop words.
        .filter {
          case ((word, _), _) => stopWords.value.contains(word) == false
        }
        .reduceByKey {
          case (count1, count2) => count1 + count2
        }
        .map {
          case ((word, path), n) => (word, (path, n))
        }
        .groupByKey  // The words are the keys
        // New: sort by Key (word).
        .sortByKey(ascending = true)
        .map {
          case (word, iterable) =>
            // New: sort the sequence by count, descending. Note that we also
            // sort by path. This is NOT necessary, but it removes randomness
            // when two "n" values are equal! It adds overhead, though.
            val seq2 = iterable.toSeq.sortBy {
              case (path, n) => (-n, path)
            }
            (word, seq2.mkString(", "))
        }
        .saveAsTextFile(out)
    } finally {
      sc.stop()
    }
  }
}
