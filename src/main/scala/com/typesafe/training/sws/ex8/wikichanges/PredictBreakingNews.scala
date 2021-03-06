package com.typesafe.training.sws.ex8.wikichanges

import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import play.api.libs.json.Json

object PredictBreakingNews {

  // In production, put this in HDFS, S3, or other resilient filesystem.
  val checkpointDirectory = "output/pbn-checkpoint"

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[3]", "Intro")

    def createContext(): StreamingContext = {
      val ssc = new StreamingContext(sc, Seconds(2))
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

    val ssc = StreamingContext.getOrCreate(checkpointDirectory, createContext _)

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true)
  }
}
