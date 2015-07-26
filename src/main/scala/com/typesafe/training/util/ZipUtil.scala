package com.typesafe.training.util
import java.io.{File, BufferedOutputStream, FileInputStream, FileOutputStream}
import java.util.zip.{ZipInputStream, ZipEntry}
import scala.util.Try

/**
 * Working with Zip files.
 */
object ZipUtil {
  private val hundredK = 100*1024

  /** Unzip a file into a directory, which is created if it doesn't exist. */
  def unzip(zipFile: File, targetDir: File): Try[File] = Try {
    if (targetDir.exists == false) targetDir.mkdirs
    val in = new ZipInputStream(new FileInputStream(zipFile))
    var entry = in.getNextEntry()
    while (entry != null) {
      val targetFile = new File(s"$targetDir${File.separator}${entry.getName}")
      if (entry.isDirectory) targetFile.mkdirs()
      else extract(in, targetFile)
      in.closeEntry()
      entry = in.getNextEntry()
    }
    in.close()
    targetDir
  }

  /** Unzip a file into a directory, which is created if it doesn't exist. */
  def unzip(zipFile: String, targetDir: String): Try[File] =
    unzip(new File(zipFile), new File(targetDir))

  /** Zip a directory. (TODO) */
  def zip(zipFile: File, sourceDir: File): Try[File] = Try { ??? }

  /** Zip a directory. (TODO) */
  def zip(zipFile: String, sourceDir: String): Try[File] =
    zip(new File(zipFile), new File(sourceDir))

  private def extract(in: ZipInputStream, targetFile: File): Try[File] = Try {
    val out = new BufferedOutputStream(new FileOutputStream(targetFile))
    val bytes = Array.fill[Byte](hundredK)(0)
    var count = in.read(bytes, 0, hundredK)
    while (count != -1) {
      out.write(bytes, 0, count)
      count = in.read(bytes, 0, hundredK)
    }
    out.close()
    targetFile
  }
}