package com.typesafe.training.sws

import com.typesafe.training.util.{Timestamp, CommandLineOptions}
import com.typesafe.training.util.CommandLineOptions.Opt
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

/** Additional command-line options for K-Means examples. */
object KMeansCommandLineOptions {

  /**
   * The "K" for K-Means, the number of clusters to find.
   */
  def k(k: Option[Int]): Opt = Opt(
    name   = "k",
    value  = k.map(_.toString),
    help   = s"-k | --k  K        The number of clusters to find with K-Means",
    parser = {
      case ("-k" | "--k") +: k +: tail => (("k", k), tail)
    })

  /**
   * Number of iterations of K-Means.
   */
  def numberIterations(n: Option[Int]): Opt = Opt(
    name   = "number-iterations",
    value  = n.map(_.toString),
    help   = s"-n | --number-iterations  N    Number of iterations.",
    parser = {
      case ("-n" | "--number-iterations") +: n +: tail => (("number-iterations", n), tail)
    })

  /**
   * K-Means initialization mode, one of "random" (default) or "k-means||"
   * ("k-means parallel").
   */
  def initializationMode(imode: Option[String]): Opt = Opt(
    name   = "initialization-mode",
    value  = imode,
    help   = s"--im | --initialization-mode  mode      Either 'random' or 'k-means||'.",
    parser = {
      case ("--im" | "--initialization-mode") +: mode +: tail => (("initialization-mode", mode), tail)
    })

}
