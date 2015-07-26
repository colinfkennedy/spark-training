// This file downloads the Divvy data set and unzips it into 'data/Divvy'.
// The URL is https: *www.divvybikes.com/datachallenge
// Hence, you'll need an internet connection to run this script.

import com.typesafe.training.data._
import com.typesafe.training.util.{FileUtil, ZipUtil}
import com.typesafe.training.util.www.Curl
import java.io.File
import scala.util.{Try, Success, Failure}

val sep       = File.separator
val divvyURL  = "https://s3.amazonaws.com/divvy-data/datachallenge/datachallenge.zip"
val zipName   = divvyURL.split("/").last
val divvyDir  = "data/Divvy"
val zipFile   = divvyDir + sep + zipName

Curl(divvyURL, divvyDir, showProgress = true) match {
  case Failure(ex) =>
    println(s"Curl failed: $ex")
  case Success(divvyDir) =>
    ZipUtil.unzip(zipFile, divvyDir) match {
      case Failure(ex) =>
        println(s"Curl failed: $ex")
      case Success(divvyDir) =>
        FileUtil.ls(divvyDir) match {
          case Nil => println("Failed to unzip Divvy data to "+divvyDir)
          case seq =>
            println(s"Contents of $divvyDir:")
            seq foreach (f => println("  "+f))
            println("\nIf the whitespace in the directory name causes problems later,")
            println("manually remove it and change the paths in the subsequent Divvy programs.")
        }
    }
}

