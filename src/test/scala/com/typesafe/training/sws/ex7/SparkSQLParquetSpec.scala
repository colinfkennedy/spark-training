package com.typesafe.training.sws.ex7
import com.typesafe.training.util.TestUtil
import org.scalatest.FunSpec
import java.io.{File, FileOutputStream, PrintStream}

class SparkSQLParquetSpec extends FunSpec {

  describe ("SparkSQLParquet") {
    it ("Read and write Parquet files") {
      val in     = "data/kjvdat.txt"
      val out    = "output/spark-sql-parquet-test"
      val golden = "golden/spark-sql-parquet-test/out.txt"
      TestUtil.testAndRemove(out)  // Delete previous runs, if necessary.

      val fileStream = new FileOutputStream(new File(out))
      SparkSQLParquet.out = new PrintStream(fileStream, true)

      SparkSQLParquet.main(Array(
        "--master", "local", "--quiet",
        "--in", in))

      TestUtil.verifyAndClean(out, golden, out)
    }
  }
}