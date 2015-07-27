package com.typesafe.training.sws.ex9

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Adapted from the Spark distro example:
 * examples/src/main/scala/org/apache/spark/examples/mllib/StreamingKMeansExample.scala
 */
object LocationStreamingKMeans {

  val batchInterval = 5
  val k             = 10
  val trainingDir   = "data/StreamingKMeans/training"
  val testDir       = "output/StreamingKMeans/test"
  val timeout       = 10
  val fromStates    = Vector("AK", "AL", "AR", "AS", "AZ", "CA", "CO", "CQ", "CT", "DC", "DE", "FL", "GA", "GU", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NA", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VI", "VT", "WA", "WI", "WV", "WY").zipWithIndex.toMap
  val toStates      = fromStates.map(_.swap)

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "Streaming KMeans")
    // Streaming...
    val ssc = new StreamingContext(sc, Seconds(batchInterval))

    try {
      // (label,[x,y])
      val testRE = """\((\w+),\[([^\]]+)\]\)""".r
      val testData = ssc.textFileStream(testDir).flatMap {
        case testRE(state,rest) =>
          val stateID = fromStates(state)
          // println(s"state: $state")
          Seq(LabeledPoint.parse(s"($stateID,[$rest])"))
        case line =>
          println(s"Bad Line: $line")
          Nil
      }

      // [x,y]
      val trainingData = ssc.textFileStream(trainingDir).map(Vectors.parse _)

      val model = new StreamingKMeans().
        setK(k).
        setDecayFactor(1.0).
        setRandomCenters(2, 10.0)

      model.trainOn(trainingData)
      model.predictOnValues(testData.map(lp => (lp.label, lp.features))) foreachRDD { (rdd, time) =>
        println(s"+++++++++++++++++++++\ntime: $time")
        val lm = model.latestModel
        println(s"Centers: ${lm.clusterCenters.toArray.toSeq}")
        println(s"Centers: ${lm.clusterWeights.toArray.toSeq}\n")
        rdd.keyBy(_._2).groupByKey.map{
          case (key, iter) => (key, iter.map(_._1))
        } foreach println
      }

      ssc.start()
      // ssc.awaitTermination()
      ssc.awaitTerminationOrTimeout(timeout * 1000)
    } finally {
      if (ssc != null) {
        ssc.stop(stopSparkContext = true, stopGracefully = true)
      }
    }
  }
}