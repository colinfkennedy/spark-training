package com.typesafe.training.sws.ex9

import com.typesafe.training.util.{FileUtil, ZipUtil}
import com.typesafe.training.util.www.Curl
import java.io.File
import scala.util.{Try, Success, Failure}

/**
 * Downloads the Divvy data set and unzips it into 'data/Divvy'.
 * See https://www.divvybikes.com/datachallenge
 * Hence, you'll need an internet connection to run this program.
 */
object DivvySetup {

  def main(args: Array[String]): Unit = {
    val success = if (run()) 0 else 1
    sys.exit(success)
  }

  val zipDir    = "output/DivvyZip"
  val divvyDir  = "data/Divvy"

  def run(targetDir: String = divvyDir): Boolean = {

    val sep       = File.separator
    val divvyURL  = "https://s3.amazonaws.com/divvy-data/datachallenge/datachallenge.zip"
    val zipName   = divvyURL.split("/").last
    val zipFile   = zipDir + sep + zipName

    Curl(divvyURL, zipDir, showProgress = true) match {
      case Failure(ex) =>
        println(s"Curl failed: $ex")
        false
      case Success(outputDir) =>
        ZipUtil.unzip(zipFile, targetDir) match {
          case Failure(ex) =>
            println(s"Unzip failed: $ex")
            false
          case Success(outputDir2) =>
            FileUtil.ls(outputDir2) match {
              case Nil =>
                println(s"Unzip appeared to succeed, but $outputDir2 is empty! (Zip file: $zipFile)")
                false
              case seq =>
                println(s"Contents of $outputDir2:")
                seq foreach (f => println("  "+f))
                println(s"""
                  |If the whitespace in the directory name causes problems later, manually remove
                  |the whitespace and change the paths in the subsequent Divvy programs.
                  |The downloaded zip file is $zipFile. You can
                  |remove it if you no longer want it.""".stripMargin)
                true
            }
        }
    }
  }
}
