package com.typesafe.training.util

sealed trait FileFormat {
  /** When specifying a command-line argument, use this string. */
  val commandLineArg: String
}

object FileFormat {
  val allowed = "text (delimiter), csv, tsv, line (no delimiter), seq (Sequence), par[quet], orc"

  val textRE = """text\(([^)]+)\)""".r
  val parquetRE = """par|parquet""".r
  def unapply(fmt: String): Option[FileFormat] = fmt match {
    case textRE(delim) => Some(Text(delim))
    case "csv"         => Some(CSV)
    case "tsv"         => Some(TSV)
    case "line"        => Some(Line)
    case "seq"         => Some(Sequence)
    case parquetRE()   => Some(Parquet)
    case "orc"         => Some(ORCFile)
    case _ => None
  }
}

/** Plain text, with a custom field delimiter string (i.e., "|"). */
case class Text(delimiter: String) extends FileFormat {
  val commandLineArg = s"text($delimiter)"
}

/** Comma-separated values */
case object CSV extends FileFormat {
  val commandLineArg = "csv"
}

/** Tab-separated values */
case object TSV extends FileFormat {
  val commandLineArg = "tsv"
}

/** Plain text, no delimiter. Treats the whole line as a "field" */
case object Line extends FileFormat {
  val commandLineArg = "line"
}

/** Sequence file format. */
case object Sequence extends FileFormat {
  val commandLineArg = "seq"
}

/** Parquet format. */
case object Parquet extends FileFormat {
  val commandLineArg = "parquet"
}

/** ORCFile format. */
case object ORCFile extends FileFormat {
  val commandLineArg = "orc"
}
