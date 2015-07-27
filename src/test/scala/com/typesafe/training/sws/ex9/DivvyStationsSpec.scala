package com.typesafe.training.sws.ex9
import com.typesafe.training.util.TestUtil
import org.scalatest.FunSpec
import java.io.{File, FileOutputStream, PrintStream}

class DivvyStationsSpec extends FunSpec with DivvyCommon {

  describe ("DivvyStations") {
    it ("Generates Divvy station lat-long data from the downloaded Divvy data") {
      val golden = defaultDivvyStationsFile.replace("output", "golden")
      TestUtil.testAndRemove(defaultDivvyOutputDir)  // Delete previous runs, if necessary.

      DivvyStations.main(Array("-q"))

      TestUtil.verifyAndClean(defaultDivvyStationsFile, golden, defaultDivvyOutputDir)
    }
  }
}