package com.typesafe.training.util
import java.io._
import scala.io.Source

/**
 * Miscellaneous file utilities.
 * They only work for the local file system, not HDFS.
 * Some are very thin wrappers around the corresponding `java.io.File` methods.
 */
object FileUtil {
  /**
   * Recursively, forcibly delete a path.
   * @return true if successful or the path doesn't exist, or return false on failure.
   */
  def rmrf(path: String): Boolean = rmrf(new File(path))

  /**
   * Recursively, forcibly delete a path.
   * @return true if successful or the path doesn't exist, or return false on failure.
   */
  def rmrf(path: File): Boolean = {
    if (path.isFile) path.delete()
    else if (path.exists) {
      path.listFiles foreach rmrf
      path.delete()
    }
    true
  }

  /**
   * Delete a path.
   * @return true if successful or the path doesn't exist, or return false on failure.
   */
  def rm(file: String): Boolean = rm(new File(file))

  /**
   * Delete a path.
   * @return true if successful or the path doesn't exist, or return false on failure.
   */
  def rm(file: File): Boolean = file.delete

  /**
   * Make a directory, including its parents as needed.
   * @return true if successful or false if not.
   */
  def mkdirs(path: String): Boolean = (new File(path)).mkdirs

  /**
   * Make a directory, including its parents as needed.
   * @return true if successful or false if not.
   */
  def mkdirs(path: File): Boolean = path.mkdirs
}