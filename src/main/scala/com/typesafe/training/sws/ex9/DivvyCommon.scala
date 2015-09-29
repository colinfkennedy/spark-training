package com.typesafe.training.sws.ex9

import com.typesafe.training.data._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.functions._  // for min, max, avg, etc.
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import java.io.File

/**
 * Utilities for the Divvy examples.
 */
trait DivvyCommon {

  def pathSep: String = File.separator
  def defaultDivvyDir: String = "data"+ pathSep + "Divvy"
  def defaultDivvyOutputDir: String = "output"+ pathSep + "Divvy"
  def defaultDivvyStationsFile: String =
    defaultDivvyOutputDir+ pathSep + "stations-lat-long"+ pathSep + "data.csv"

  /**
   * Format each double for output to 5 decimal points.
   */
  protected def formatDoubles(ds: Double*): String =
    ds.toSeq.map(d => f"$d%.5f").mkString(",")

  /**
   * Shift a scaled coordinate back to the original.
   */
  protected def unscale (coord: Double, delta: Double, avg: Double): Double = (delta * coord)  + avg

}