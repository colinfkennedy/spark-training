package com.typesafe.training.sws.ex7
import com.typesafe.training.util.TestUtil
import org.scalatest.FunSpec
import java.io.{File, FileOutputStream, PrintStream}

class SparkDataFramesSpec extends FunSpec {

  describe ("SparkDataFrames") {
    it ("Use DataFrames to query the airline data") {
      val in     = "data/airline-flights/alaska-airlines/2008.csv"
      val out    = "output/spark-df-test"
      val golden = "golden/spark-df-test/out.txt"
      TestUtil.testAndRemove(out)  // Delete previous runs, if necessary.

      val fileStream = new FileOutputStream(new File(out))
      SparkDataFrames.out = new PrintStream(fileStream, true)

      SparkDataFrames.main(Array(
        "--master", "local", "--quiet",
        "--in", in))

      TestUtil.verifyAndClean(out, golden, out)
    }
  }
}