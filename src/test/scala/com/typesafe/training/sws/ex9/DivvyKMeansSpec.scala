package com.typesafe.training.sws.ex9
import com.typesafe.training.util.TestUtil
import org.scalatest.FunSpec
import java.io.{File, FileOutputStream, PrintStream}
import scala.io.Source

class DivvyKMeansSpec extends FunSpec with DivvyCommon {

  describe ("DivvyKMeans") {
    it ("Finds clusters in Divvy station lat-long data using K-Means") {
      val divvyOutDir = "output/Divvy"
      val csvOutFile = divvyOutDir + "/centroids-10/part-00000"
      val csvGoldenFile = csvOutFile.replace("output", "golden")
      TestUtil.testAndRemove(divvyOutDir)  // Delete previous runs, if necessary.

      DivvyKMeans.main(Array("-q"))

      // Because of the random nature of K-Means, we don't get reproducible
      // results, so we satisfy ourselves with "basic" checks.
      val (outLineCount, outLatMin, outLatMax, outLongMin, outLongMax) = findExtrema(csvOutFile)
      val (goldenLineCount, goldenLatMin, goldenLatMax, goldenLongMin, goldenLongMax) = findExtrema(csvGoldenFile)
      assert (outLineCount === goldenLineCount)
      assert (outLatMin  > goldenLatMin  - 0.1)
      assert (outLongMin > goldenLongMin - 0.1)
      assert (outLatMax  < goldenLatMax  + 0.1)
      assert (outLongMin > goldenLongMin - 0.1)
      TestUtil.testAndRemove(divvyOutDir)
    }
  }

  val seed = -361.0

  protected def findExtrema(file: String): (Int, Double, Double, Double, Double) =
    Source.fromFile(file).getLines.foldLeft((0, seed, -seed, seed, -seed)) {
      case ((lineNum, latMin, latMax, longMin, longMax), line) =>
        line.split(",") match {
          case Array(latStr, longStr) =>
            val (lat, long) = (latStr.toDouble, longStr.toDouble)
            val newLatMin  = if (latMin  > lat)  lat  else latMin
            val newLatMax  = if (latMax  < lat)  lat  else latMax
            val newLongMin = if (longMin > long) long else longMin
            val newLongMax = if (longMax < long) long else longMax
            (lineNum+1, newLatMin, newLatMax, newLongMin, newLongMax)
          case _ =>
            fail(s"Bad input line from $file: $line")
        }
    }
}