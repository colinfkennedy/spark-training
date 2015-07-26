package com.typesafe.training.sws.ex6
import com.typesafe.training.util.{Timestamp, TestUtil}
import org.scalatest.FunSpec

class JoinsSpec extends FunSpec {

  describe ("Joins") {
    it ("computes the join of the airline data with airport data") {
      Timestamp.isTest = true
      val in      = "data/airline-flights/alaska-airlines/2008.csv"
      val out     = "output/airlines-flights-airports-join"
      val out2    = out+"-"
      val golden  = "golden/airlines-flights-airports-join/part-00000"
      TestUtil.testAndRemove(out)  // Delete previous runs, if necessary.

      Joins.main(Array(
        "--master", "local", "--quiet",
        "--input-path", in,
        "--airports", "data/airline-flights/airports.csv",
        "--output-path", out))

      TestUtil.verifyAndClean(s"$out2/part-00000", golden, out2)
    }
  }
}