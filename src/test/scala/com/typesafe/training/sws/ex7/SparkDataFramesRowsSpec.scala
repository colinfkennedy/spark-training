package com.typesafe.training.sws.ex7
import com.typesafe.training.util.TestUtil
import org.scalatest.FunSpec
import java.io.{File, FileOutputStream, PrintStream}

class SparkDataFramesRowsSpec extends FunSpec {

  describe ("SparkDataFramesRows") {
    it ("Use DataFrames with Rows and Schemas to query the airport data") {
      val airportsPath = "data/airline-flights/airports.csv"
      val out    = "output/spark-df-rows-test"
      val golden = "golden/spark-df-rows-test/out.txt"
      TestUtil.testAndRemove(out)  // Delete previous runs, if necessary.

      val fileStream = new FileOutputStream(new File(out))
      SparkDataFramesRows.out = new PrintStream(fileStream, true)

      SparkDataFramesRows.main(Array(
        "--master", "local", "--quiet",
        "--airports", airportsPath))

      TestUtil.verifyAndClean(out, golden, out)
    }
  }
}