package com.typesafe.training.sws.ex6.solns
import com.typesafe.training.util.{Timestamp, TestUtil}
import org.scalatest.FunSpec

class JoinsOrderedSpec extends FunSpec {

  describe ("JoinsOrdered") {
    it ("computes the join of the airline data with airport data, then restores the order by timestamp") {
      Timestamp.isTest = true
      val in      = "data/airline-flights/alaska-airlines/2008.csv"
      val out     = "output/airlines-flights-airports-join-ordered"
      val out2    = out+"-"
      val golden  = "golden/airlines-flights-airports-join-ordered/part-00000"
      TestUtil.testAndRemove(out2)  // Delete previous runs, if necessary.

      JoinsOrdered.main(Array(
        "--master", "local", "--quiet",
        "--input-path", in,
        "--airports", "data/airline-flights/airports.csv",
        "--output-path", out))

      TestUtil.verifyAndClean(s"$out2/part-00000", golden, out2)
    }
  }
}
