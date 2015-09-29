package com.typesafe.training.sws.ex2
import com.typesafe.training.util.{Timestamp, TestUtil}
import org.scalatest.FunSpec

class WordCountBSpec extends FunSpec {

  describe ("WordCountB") {
    it ("computes the word count of the input corpus") {
      Timestamp.isTest = true
      val out    = "output/shakespeare-wcB-"
      val golden = "golden/shakespeare-wcB/data.txt"
      TestUtil.testAndRemove(out)  // Delete previous runs, if necessary.

      WordCountA.main(Array.empty[String])

      TestUtil.verifyAndClean(out, golden, out)
    }
  }
}