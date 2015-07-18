package com.typesafe.training.sws.ex5
import com.typesafe.training.util.{Timestamp, TestUtil}
import org.scalatest.FunSpec
import java.io.{File, FileOutputStream, PrintStream}

class NGramsSpec extends FunSpec {

  describe ("NGrams") {
    it ("computes ngrams from text") {
      Timestamp.isTest = true
      val out    = "output/ngrams.txt"
      val golden = "golden/ngrams/100.txt"
      TestUtil.testAndRemove(out)  // Delete previous runs, if necessary.

      val fileStream = new FileOutputStream(new File(out))
      NGrams.out = new PrintStream(fileStream, true)

      NGrams.main(Array(
        "--master", "local", "--quiet",
        "--input-path", "data/all-shakespeare.txt",
        "--count", "100", "--ngrams", "% love % %"))

      TestUtil.verifyAndClean(out, golden, out)
    }
  }
}