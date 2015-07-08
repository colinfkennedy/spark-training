package com.typesafe.training.sws.ex4

import com.typesafe.training.util.{Timestamp, CommandLineOptions}
import java.io.{File, FilenameFilter}
import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Simulate a web crawl to prep. data for InvertedIndex.
 * Crawl uses <code>SparkContext.wholeTextFiles</code> to read the files
 * in a directory hierarchy and return a single RDD with records of the form:
 *    <code>(file_name, file_contents)</code>
 * After loading with <code>SparkContext.wholeTextFiles</code>, we post process
 * the data in two ways. First, we the <code>file_name</code> will be an
 * absolute path, which is normally what you would want. However, to make it
 * easier to support running the corresponding unit test <code>CrawlSpec</code>
 * anywhere, we strip all leading path elements. Second, the <code>file_contents</code>
 * still contains linefeeds. We remove those, so that <code>InvertedIndex</code>
 * can treat each line as a complete record.
 */
object Crawl {
  def main(args: Array[String]): Unit = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName, "",
      CommandLineOptions.inputPath(Some("data/enron-spam-ham/*")), // Note the *
      CommandLineOptions.outputPath(Some("output/crawl")),
      CommandLineOptions.master(Some("local[*]")),
      CommandLineOptions.quiet)
    val argz = options(args.toList)

    val separator = java.io.File.separator

    val sc = new SparkContext(argz("master"), "Crawl")

    try {
      // See class notes above.
      val files_contents = sc.wholeTextFiles(argz("input-path")).map {
        case (id, text) =>
          val lastSep = id.lastIndexOf(separator)
          val id2 = if (lastSep < 0) id.trim else id.substring(lastSep+1, id.length).trim
          val text2 = text.trim.replaceAll("""\s*\n\s*""", " ")
          (id2, text2)
      }
      val out = argz("output-path")
      if (! argz.getOrElse("quiet", "false").toBoolean)
        println(s"Writing output to: $out")
      files_contents.saveAsTextFile(out)
    } finally {
      sc.stop()
    }
  }
}