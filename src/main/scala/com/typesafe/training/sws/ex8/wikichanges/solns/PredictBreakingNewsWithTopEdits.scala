package com.typesafe.training.sws.ex8.wikichanges.solns

import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import play.api.libs.json.Json

object PredictBreakingNewsWithTopEdits {

  // In production, put this in HDFS, S3, or other resilient filesystem.
  val checkpointDirectory = "output/pbn2-checkpoint"

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[3]", "Intro")

    def createContext(): StreamingContext = {
      val ssc = new StreamingContext(sc, Seconds(1))
      ssc.checkpoint(checkpointDirectory)
      val wikiChanges = ssc.socketTextStream("localhost", 8124)

      val urlAndCount: DStream[(String, Int)] = wikiChanges
        .flatMap(_.split("\n"))
        .map(Json.parse(_))
        .map(j => (j \ "pageUrl").as[String] -> 1)

      val topEdits = urlAndCount.reduceByKeyAndWindow(
        reduceFunc = _ + _,
        invReduceFunc = _ - _,
        windowDuration = Seconds(2 * 60),
        filterFunc = _._2 > 0)
        .transform(_.sortBy(_._2, ascending = false))

      topEdits.print(20)
      ssc
    }

    // The preferred way to construct a StreamingContext, because if the
    // program is restarted, e.g., due to a crash, it will pick up where it
    // left off. Otherwise, it starts a new job.
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, createContext _)

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true)
  }
}
