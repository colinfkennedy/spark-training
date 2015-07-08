package com.typesafe.training.sws.ex7
import com.typesafe.training.util.TestUtil
import org.scalatest.FunSpec
import java.io.{File, FileOutputStream, PrintStream}

class SparkSQLSpec extends FunSpec {

  describe ("SparkSQL") {
    it ("Use SQL to query the airline data") {
      val in     = "data/airline-flights/alaska-airlines/2008.csv"
      val out    = "output/spark-sql-test"
      val golden = "golden/spark-sql-test/out.txt"
      TestUtil.testAndRemove(out)  // Delete previous runs, if necessary.

      val fileStream = new FileOutputStream(new File(out))
      SparkSQL.out = new PrintStream(fileStream, true)

      SparkSQL.main(Array("--quiet", "--in", in))

      TestUtil.verifyAndClean(out, golden, out)
    }
  }
}