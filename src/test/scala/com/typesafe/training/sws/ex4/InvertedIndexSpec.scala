package com.typesafe.training.sws.ex4
import com.typesafe.training.util.{Timestamp, TestUtil}
import org.scalatest.FunSpec

class InvertedIndexSpec extends FunSpec {

  describe ("InvertedIndex") {
    it ("computes the famous 'inverted index' from web crawl data") {
      Timestamp.isTest = true
      val out    = "output/inverted-index"
      val out2   = s"$out-/part-00000"
      val golden = "golden/inverted-index/part-00000"
      TestUtil.testAndRemove(out + "-")  // Delete previous runs, if necessary.

      // We have to run Crawl first to ensure the data exists!
      // Note that we use a single core (local).
      TestUtil.testAndRemove("output/crawl")  // Delete previous runs, if necessary.
      Crawl.main(Array("--master", "local", "--quiet"))

      InvertedIndex.main(Array(
        "--master", "local", "--quiet",
        "--input-path", "output/crawl",
        "--output-path", out))

      TestUtil.verifyAndClean(out2, golden, out+"-") //, sortLines = true)
      TestUtil.testAndRemove("output/crawl")
    }
  }
}
