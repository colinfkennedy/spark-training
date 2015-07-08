package com.typesafe.training.sws.ex8

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SQLContext

case class Crawl(path: String, text: String)
object Crawl {
  // Format for the "crawl" data: (document_path, text)
  val rawRE = """^\s*\(([^,]+),(.*)\)\s*$""".r

  def parse(raw: String): Option[Crawl] = raw match {
    case rawRE(path, text) => Some(Crawl(path, text))
    case _ => println("ERROR: Failed to parse "+raw); None
  }
}

object StreamingInvertedIndex {

  def main(args: Array[String]): Unit = {
    val master   = "local[*]"
    val interval = Seconds(5)
    val server   = "localhost"
    val port     = 9900
    val input    = "output/crawl"
    val output   = "output/inverted-index-streaming"
    run(master, interval, server, port, input, output)
  }

  def run(master: String, interval: Duration, server: String, port: Int,
    input: String, output: String): Unit = {

    val sparkContext = new SparkContext(master, "Inverted Index")
    // To keep this example simpler, we don't use the recommended
    // StreamingContext.getOrCreate():
    val streamingContext = new StreamingContext(sparkContext, interval)
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._

    val dstream = streamingContext.socketTextStream(server, port)
    val crawls = for {
      line <- dstream
      crawl <- Crawl.parse(line)
    } yield crawl

    crawls
      .flatMap {
        case Crawl(path, text) =>
          text.trim.split("""[^\w']""") map (word => ((word, path), 1))
      }
      .reduceByKey{
        case (count1, count2) => count1 + count2
      }
      .foreachRDD {
        _.groupBy {
          case ((word, path), n) => word
        }
        // .map {
        //   case (word, seq) =>
        //     val seq2 = seq map {
        //       case (redundantWord, (path, n)) => (path, n)
        //     }.sortBy {
        //       case (path, n) => (-n, path)
        //     }
        //     (word, seq2.mkString(", "))
        // }
        .take(20).foreach(println)
        // .saveAsTextFiles(out)
      }

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(2*1000)
    streamingContext.stop()
    dstream.stop()
  }
}
