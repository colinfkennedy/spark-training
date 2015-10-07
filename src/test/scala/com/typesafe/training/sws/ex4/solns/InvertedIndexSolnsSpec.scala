package com.typesafe.training.sws.ex4.solns
import com.typesafe.training.util.{Timestamp, TestUtil}
import com.typesafe.training.sws.ex4.Crawl
import org.scalatest.FunSpec

class InvertedIndexSolnsSpec extends FunSpec {

  def setup(name: String) = {
    Timestamp.isTest = true
    TestUtil.testAndRemove(s"output/$name-")  // Delete previous runs, if necessary.
    // We have to run Crawl first to ensure the data exists!
    // Use a single core (local).
    TestUtil.testAndRemove("output/crawl")  // Delete previous runs, if necessary.
    Crawl.main(Array("--quiet", "--master", "local"))
  }

  def verifyAndClean(name: String) = {
    val out    = s"output/$name"
    val out2   = s"$out-/part-00000"
    val golden = s"golden/$name/part-00000"

    TestUtil.verifyAndClean(out2, golden, out+"-")
    TestUtil.testAndRemove("output/crawl")
  }

  describe ("InvertedIndexSortByWordsAndCounts") {
    it ("computes the famous 'inverted index' from web crawl data with sorting") {
      val name = "inverted-index-sorted"
      setup(name)
      InvertedIndexSortByWordsAndCounts.main(Array(
        "--master", "local", "--quiet",
        "--input-path", "output/crawl",
        "--output-path", s"output/$name"))
      verifyAndClean(name)
    }
  }

  describe ("InvertedIndexSortByWordsAndCountsWithStopWordsFiltering") {
    it ("computes the famous 'inverted index' from web crawl data with sorting") {
      val name = "inverted-index-sorted-stop-words-removed"
      setup(name)
      InvertedIndexSortByWordsAndCountsWithStopWordsFiltering.main(Array(
        "--master", "local", "--quiet",
        "--input-path", "output/crawl",
        "--output-path", s"output/$name"))
      verifyAndClean(name)
    }
  }
}
