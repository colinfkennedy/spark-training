package com.typesafe.training.util.www
import java.net.URL
import java.io.{File, BufferedInputStream, BufferedOutputStream, FileOutputStream}
import scala.util.Try

/**
 * Behaves like the a crude version of the *NIX utility "curl".
 */
object Curl {
  /** If successful, return the output file `File`. */
  def apply(
    sourceURL: URL,
    targetDirectory: File,
    showProgress: Boolean): Try[File] = Try {
      val sourceFileName = sourceURL.getFile.split("/").last
      val pathSep = File.separator
      val outFileName = targetDirectory.getPath + pathSep + sourceFileName

      if (showProgress) println(s"Downloading $sourceURL to $outFileName")
      val connection = sourceURL.openConnection()
      val in = new BufferedInputStream(connection.getInputStream())
      // If here, connection is successfully open, create the
      // target directory if necessary.
      targetDirectory.mkdirs()

      val outFile = new File(outFileName)
      val out = new BufferedOutputStream(new FileOutputStream(outFile))
      val hundredK = 100*1024
      val bytes = Array.fill[Byte](hundredK)(0)
      var loops = 0
      var count = in.read(bytes, 0, hundredK)
      while (count != -1) {
        if (showProgress && loops % 10 == 0) print(".")
        loops += 1
        out.write(bytes, 0, count)
        count = in.read(bytes, 0, hundredK)
      }
      if (showProgress) println()
      in.close()
      out.flush()
      out.close()
      outFile
    }

  def apply(
    sourceURLString: String,
    targetDirectoryString: String,
    showProgress: Boolean = true): Try[File] =
      apply(
        new URL(sourceURLString),
        new File(targetDirectoryString),
        showProgress)
}
