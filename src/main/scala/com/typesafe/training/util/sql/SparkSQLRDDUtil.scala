package com.typesafe.training.util.sql

import com.typesafe.training.data._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Helper class for the Spark SQL exercises that loads the commercial avaiation
 * data set as RDDs.
 */
object SparkSQLRDDUtil {
  def load(
    sc:           SparkContext,
    flightsPath:  String,
    carriersPath: String,
    airportsPath: String,
    planesPath:   String): (RDD[Flight], RDD[Carrier], RDD[Airport], RDD[Plane]) = {

    // Don't "pre-guess" keys; just use the types as Schemas.
    val flights = for {
      line <- sc.textFile(flightsPath)
      flight <- Flight.parse(line)
    } yield flight

    // The carriers isn't very useful if you load just the Alaska Airlines data.
    val carriers = for {
      line <- sc.textFile(carriersPath)
      carrier <- Carrier.parse(line)
    } yield carrier

    val airports = for {
      line <- sc.textFile(airportsPath)
      airport <- Airport.parse(line)
    } yield airport

    val planes = for {
      line <- sc.textFile(planesPath)
      plane <- Plane.parse(line)
    } yield plane

    (flights, carriers, airports, planes)
  }
}

