package com.typesafe.training.sws.ex6.solns
import com.typesafe.training.util.{Timestamp, TestUtil}
import org.scalatest.FunSpec

class KJVJoinsOrderedSpec extends FunSpec {

  describe ("KJVJoinsOrdered") {
    it ("computes the join of the bible book abbreviations with their full names") {
      Timestamp.isTest = true
      val out     = "output/kjv-joins-ordered"
      val out2    = out+"-"
      val golden  = "golden/kjv-joins-ordered/part-00000"
      TestUtil.testAndRemove(out)  // Delete previous runs, if necessary.

      KJVJoinsOrdered.main(Array(
        "--quiet", "--input-path", "data/kjvdat.txt",
        "--abbreviations", "data/abbrevs-to-names.tsv", "--output-path", out))

      TestUtil.verifyAndClean(s"$out2/part-00000", golden, out2)
    }
  }
}