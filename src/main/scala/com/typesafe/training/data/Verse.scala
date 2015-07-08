package com.typesafe.training.data

/**
 * A case class used for the exercises involving SQL APIs. It represents a
 * "record" for a a verse from the King James Version of the Bible, as well as
 * the verses in some of the other sacred texts in the data directory.
 * Recall that each line has the format:
 *   <pre><code>book|chapter|verse| text.~</code></pre>
 * We use a case class to define the schema, as required by Spark SQL.
 */
case class Verse(book: String, chapter: Int, verse: Int, text: String)
object Verse {
  // Regex to match the fields separated by "|".
  // Also strips the trailing "~" in the KJV file.
  val lineRE = """^\s*([^|]+)\s*\|\s*([\d]+)\s*\|\s*([\d]+)\s*\|\s*(.*)~?\s*$""".r
  def parse(s: String): Option[Verse] = s match {
    case lineRE(book, chapter, verse, text) =>
      Some(Verse(book.trim, chapter.toInt, verse.toInt, text.trim))
    case line =>
      Console.err.println(s"ERROR: Invalid verse line: $line")
      None
  }
}
