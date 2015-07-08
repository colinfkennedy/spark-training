package com.typesafe.training.sws.ex2.solns
import com.typesafe.training.util.{Timestamp, TestUtil}
import org.scalatest.FunSpec

class WordCountSolnsSpec extends FunSpec {

  def setup(name: String) = {
    Timestamp.isTest = true
    TestUtil.testAndRemove(s"output/${name}-")  // Delete previous runs, if necessary.
  }

  def verifyAndClean(name: String) = {
    val out    = s"output/$name"
    val out2   = s"$out-/part-00000"
    val golden = s"golden/$name/part-00000"

    TestUtil.verifyAndClean(out2, golden, out+"-")
  }

  describe ("WordCountSortByWord") {
    it ("computes the word count of the input, sorted by word") {
      val name = "shakespeare-wc-sort-by-word"
      setup(name)
      WordCountSortByWord.main(Array(
        "--quiet", "--input-path", "data/all-shakespeare.txt", "--output-path", s"output/$name"))
      verifyAndClean(name)
    }
  }

  describe ("WordCountSortByCount") {
    it ("computes the word count of the input, sorted by count") {
      val name = "shakespeare-wc-sort-by-count"
      setup(name)
      WordCountSortByCount.main(Array(
        "--quiet", "--input-path", "data/all-shakespeare.txt", "--output-path", s"output/$name"))
      verifyAndClean(name)
    }
  }

  describe ("WordCountSortByFirstLetter") {
    it ("computes the word count of the input, counted and sorted by first letter") {
      val name = "shakespeare-wc-sort-by-count-first-letter"
      setup(name)
      WordCountSortByFirstLetter.main(Array(
        "--quiet", "--input-path", "data/all-shakespeare.txt", "--output-path", s"output/$name"))
      verifyAndClean(name)
    }
  }

  describe ("WordCountSortByWordLength") {
    it ("computes the word count of the input, sorted by word length") {
      val name = "shakespeare-wc-sort-by-word-length"
      setup(name)
      WordCountSortByWordLength.main(Array(
        "--quiet", "--input-path", "data/all-shakespeare.txt", "--output-path", s"output/$name"))
      verifyAndClean(name)
    }
  }

  describe ("WordCountGroupByWordCounts") {
    it ("computes the word count of the input, sorted by count") {
      val name = "shakespeare-wc-sort-by-word-count-groups"
      setup(name)
      WordCountGroupByWordCounts.main(Array(
        "--quiet", "--input-path", "data/all-shakespeare.txt", "--output-path", s"output/$name"))
      verifyAndClean(name)
    }
  }
}