package com.typesafe.training.sws.ex2
import com.typesafe.training.util.{Timestamp, TestUtil}
import org.scalatest.FunSpec

class WordCountSpec extends FunSpec {

  describe ("WordCount") {
    it ("computes the word count of the input corpus with options") {
      Timestamp.isTest = true
      val out    = "output/shakespeare-wc"
      val out2   = s"$out-/part-00000"
      val golden = "golden/shakespeare-wc/part-00000"
      TestUtil.testAndRemove(out+"-")  // Delete previous runs, if necessary.

      WordCount.main(Array(
        "--master", "local", "--quiet",
        "--input-path", "data/all-shakespeare.txt",
        "--output-path", out))

      TestUtil.verifyAndClean(out2, golden, out)
    }
  }
}