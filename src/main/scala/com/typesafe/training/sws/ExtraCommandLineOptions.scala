package com.typesafe.training.sws

import com.typesafe.training.util.{Timestamp, CommandLineOptions}
import com.typesafe.training.util.CommandLineOptions.Opt
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

/** Additional command-line options for specific examples. */
object ExtraCommandLineOptions {

  /**
   * A lookup table of abbreviations and full names. Used for the "sacred texts".
   */
  def abbrevs(abbrevsFile: Option[String]): Opt = Opt(
    name   = "abbreviations",
    value  = abbrevsFile,
    help   = s"-a | --abbreviations  path        The dictionary of abbreviations to full names.",
    parser = {
      case ("-a" | "--abbreviations") +: path +: tail => (("abbreviations", path), tail)
    })

  /**
   * Airport data
   */
  def airports(airportsFile: Option[String]): Opt = Opt(
    name   = "airports",
    value  = airportsFile,
    help   = s"-a | --airports  path             The data for airports.",
    parser = {
      case ("-a" | "--airports") +: path +: tail => (("airports", path), tail)
    })

  /**
   * Carrier data
   */
  def carriers(carriersFile: Option[String]): Opt = Opt(
    name   = "carriers",
    value  = carriersFile,
    help   = s"-c | --carriers  path             The data for carriers.",
    parser = {
      case ("-c" | "--carriers") +: path +: tail => (("carriers", path), tail)
    })

  /**
   * Plane data
   */
  def planes(planesFile: Option[String]): Opt = Opt(
    name   = "planes",
    value  = planesFile,
    help   = s"-p | --planes  path               The data for planes.",
    parser = {
      case ("-p" | "--planes") +: path +: tail => (("planes", path), tail)
    })

  /** Experimental features */
  def tungstenMode: Opt = Opt(
    name   = "tungstenMode",
    value  = None,
    help   = s"-tug | --tungsten-mode                Enable Project Tungsten features.",
    parser = {
      case ("-tug" | "--tungsten-mode") +: tail => (("tungstenMode", "true"), tail)
    })

}
