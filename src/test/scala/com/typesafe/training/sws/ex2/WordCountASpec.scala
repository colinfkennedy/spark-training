package com.typesafe.training.sws.ex2
import com.typesafe.training.util.{Timestamp, TestUtil}
import org.scalatest.FunSpec

class WordCountASpec extends FunSpec {

  describe ("WordCountA") {
    it ("computes the word count of the input corpus") {
      Timestamp.isTest = true
      val out    = "output/shakespeare-wcA-"
      val golden = "golden/shakespeare-wcA/data.txt"
      TestUtil.testAndRemove(out)  // Delete previous runs, if necessary.

      WordCountA.main(Array.empty[String])

      TestUtil.verifyAndClean(out, golden, out)
    }
  }
}