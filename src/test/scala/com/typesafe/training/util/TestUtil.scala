package com.typesafe.training.util
import java.io._
import scala.io.Source

object TestUtil {

  case class DeletionFailed(path: File) extends RuntimeException(
    s"Could not delete $path")

  // Because of random variances in processing, we
  // sort the lines, then optionally sort the text within the
  // lines.
  def verifyAndClean(actualFile: String, expectedFile: String,
      dirToDelete: String, sortLines: Boolean = false) =
    try {
      val actual   = Source.fromFile(actualFile).getLines.toSeq.sorted
      val expected = Source.fromFile(expectedFile).getLines.toSeq.sorted
      (actual zip expected).zipWithIndex foreach {
        case ((a, e), i) =>
          val a2 = if (sortLines) a.sorted else a
          val e2 = if (sortLines) e.sorted else e
          assert(a2 == e2, s"""
            |lines not equal at line #${i+1}:
            |  actual:   $a
            |  expected: $e""".stripMargin)
      }
    } finally {
      rmrf(dirToDelete)
    }

  def testAndRemove(root: String): Unit = testAndRemove(new File(root))

  def testAndRemove(root: File): Unit =
    if (root.exists() && rmrf(root) == false) throw DeletionFailed(root)

  def rmrf(root: String): Boolean = rmrf(new File(root))

  def rmrf(root: File): Boolean =
    if (root.isFile) root.delete()
    else if (root.exists) {
      (root.listFiles foldLeft true) { (status, file) =>
        if (status == false) false else rmrf(file)
      }
      root.delete()
    }
    else true

  def rm(file: String): Boolean = rm(new File(file))

  def rm(file: File): Boolean = file.delete
}