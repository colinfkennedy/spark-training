package com.typesafe.training.sws.ex4
import com.typesafe.training.util.{Timestamp, TestUtil}
import org.scalatest.FunSpec

/** Only tests the local version of Crawl. */
class CrawlSpec extends FunSpec {

  describe ("Crawl") {
    it ("simulates a web crawler") {
      Timestamp.isTest = true
      val out    = "output/crawl"
      val out2   = s"$out/part-00000"
      val golden = "golden/crawl/part-00000"
      TestUtil.testAndRemove(out)  // Delete previous runs, if necessary.

      // Note that we use a single core (local).
      // The defaults for --input-path is fine.
      Crawl.main(Array(
        "--master", "local", "--quiet",
        "--output-path", out))

      TestUtil.verifyAndClean(out2, golden, out)
    }
  }
}