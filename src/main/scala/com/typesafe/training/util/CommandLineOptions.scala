package com.typesafe.training.util

import org.apache.spark.streaming.{Duration, Seconds}
import CommandLineOptions.{Opt, NameValue, Parser}

/**
 * Handles command-line argument processing for scripts that take
 * help, input, and output arguments.
 */
case class CommandLineOptions(mainObject: AnyRef, extraHelp: String, opts: Opt*) {

  private def toHelp(opt: Opt): String =
    if (opt.value == None) opt.help
    else opt.help + s"\n                                    (default: ${opt.value.get})"

  val programName = mainObject.getClass.getName.replace("$", "")

  // Help message
  def helpMsg = s"""
    |usage: scala -cp ... $programName [options]
    |
    |where the options are the following:
    |
    |  -h | --help                       Show this message and quit.
    |""".stripMargin +
      opts.map(opt => toHelp(opt)).mkString("  ", "\n  ", "") +
      "\n\n" + extraHelp + "\n"

  lazy val matchers: Parser =
    (opts foldLeft help) {
      (partialfunc, opt) => partialfunc orElse opt.parser
    } orElse noMatch

  protected def processOpts(args: Seq[String]): Seq[NameValue] =
    args match {
      case Nil => Nil
      case _ =>
        val (newArg, rest) = matchers(args)
        newArg +: processOpts(rest)
    }

  def apply(args: Seq[String]): Map[String,String] = {
    val foundOpts = processOpts(args)
    // Construct a map of the default args, then override with the actuals
    val optsWithDefaults = for {
      opt <- opts
      value <- opt.value
    } yield (opt.name -> value)
    val map1 = optsWithDefaults.toMap
    // The actuals are already key-value pairs:
    val map2 = foundOpts.toMap
    val finalMap = map1 ++ map2
    printMap(finalMap)
    finalMap
  }

  protected def printMap(finalMap: Map[String,String]): Unit =
    for {
      boolString <- finalMap.get("quiet")
      if boolString != "true"
    } {
      println(s"$programName:")
      finalMap foreach {
        case (key, value) => printf("  %15s: %s\n", key, value)
      }
    }

  /**
   * Common argument: help
   */
  val help: Parser = {
    case ("-h" | "--h" | "--help") +: tail => quit("", 0)
  }

  /** No match! */
  val noMatch: Parser = {
    case head +: tail =>
      quit(s"Unrecognized argument (or missing second argument): $head", 1)
  }

  def quit(message: String, status: Int): Nothing = {
    if (message.length > 0) println(message)
    println(helpMsg)
    sys.exit(status)
  }
}

object CommandLineOptions {

  type NameValue = (String, String)
  type Parser = PartialFunction[Seq[String], (NameValue, Seq[String])]

  case class Opt(
    /** Used as a map key in the returned options. */
    name:  String,
    /** Initial value is the default, or None; new option instance has the actual. */
    value: Option[String],
    /** Help string displayed if user asks for help. */
    help:  String,
    /** Attempt to parse input words. If successful, return new value, rest of args. */
    parser: Parser)

  /** Common argument: The input path */
  def inputPath(value: Option[String] = None): Opt = Opt(
    name   = "input-path",
    value  = value,
    help   = "-i | --in  | --input-path  path   The input root directory of files to crawl.",
    parser = {
      case ("-i" | "--in" | "--input-path") +: path +: tail => (("input-path", path), tail)
    })

  /** Common argument: The output path */
  def outputPath(value: Option[String] = None): Opt = Opt(
    name   = "output-path",
    value  = value,
    help   = "-o | --out | --output-path path   The output location.",
    parser = {
      case ("-o" | "--out" | "--output-path") +: path +: tail => (("output-path", path), tail)
    })

  /**
   * Common argument: The input file format.
   * Not all are necessarily supported by Spark or the the application in question.
   */
  def inputFormat(value: Option[String] = Some("line")): Opt = Opt(
    name   = "input-format",
    value  = value,
    help   = s"--if | --input-format  format     One of: ${FileFormat.allowed}.",
    parser = {
      case ("--if" | "--input-format") +: FileFormat(fmt) +: tail =>
        (("input-format", fmt.commandLineArg), tail)
    })

  /**
   * Common argument: The output file format.
   * Not all are necessarily supported by Spark or the the application in question.
   */
  def outputFormat(value: Option[String] = Some("line")): Opt = Opt(
    name   = "output-format",
    value  = value,
    help   = s"--of | --output-format format     One of: ${FileFormat.allowed}.",
    parser = {
      case ("--of" | "--output-format") +: FileFormat(fmt) +: tail =>
        (("output-format", fmt.commandLineArg), tail)
    })

  val defaultMaster = "local[*]"

  /** Common argument: The Spark "master" */
  def master(value: Option[String] = None): Opt = Opt(
    name   = "master",
    value  = value,
    help   = s"""
       |  -m | --master M                   The "master" argument passed to SparkContext. "M" is one of:
       |                                    "local", local[N]", "mesos://host:port", or "spark://host:port".
       |                                   """.stripMargin,
    parser = {
      case ("-m" | "--master") +: master +: tail => (("master", master), tail)
    })

  /** Common argument: Quiet suppresses some print statements. */
  def quiet: Opt = Opt(
    name   = "quiet",
    value  = None,
    help   = s"-q | --quiet                      Suppress some informational output.",
    parser = {
      case ("-q" | "--quiet") +: tail => (("quiet", "true"), tail)
    })

  /**
   * Use "--socket host:port" for listening for events.
   */
  def socket(hostPort: Option[String]): Opt = Opt(
    name   = "socket",
    value  = hostPort,
    help   = s"-s | --socket host:port           Listen to a socket for events.",
    parser = {
      case ("-s" | "--socket") +: hp +: tail => (("socket", hp), tail)
    })

  /**
   * Use "--timeout timeout" to stop automatically after timeout seconds.
   */
  def timeout(amount: Option[Int] = None): Opt = Opt(
    name   = "timeout",
    value  = amount map (_.toString),
    help   = s"-t | --timeout N                  Terminate after N seconds.",
    parser = {
      case ("-t" | "--timeout") +: tm +: tail => (("timeout", tm), tail)
    })

  /**
   * Use "--no-term" to keep this process running "forever" (or until ^C).
   */
  def noterm(): Opt = Opt(
    name   = "no-term",
    value  = None, // ignored
    help   = s"-n | --no | --no-term             Run forever.",
    parser = {
      case ("-n" | "--no" | "--no-term") +: tail => (("no-term", "true"), tail)
    })

  val DEFAULT_INTERVAL = 5  // 5 second

  /**
   * Set the batch interval in seconds for each streaming "batch", "--interval n".
   */
  def interval(amount: Option[Duration] = Some(Seconds(DEFAULT_INTERVAL))): Opt = Opt(
    name   = "interval",
    value  = amount map (d => (d.milliseconds / 1000).toString),  // Convert from milliseconds
    help   = s"--int | --interval i              Batch duration is N seconds.",
    parser = {
      case ("--int" | "--interval") +: n +: tail => (("interval", n), tail)
    })

  val DEFAULT_WINDOW   = 5 * DEFAULT_INTERVAL   // 5-batch windows

  def toWindowSlide(windowSlideString: String): (Duration, Duration) = {
    val (w, s) = windowSlideString.split(":") match {
      case Array(n, m) => (n.toInt,        m.toInt)
      case Array("")   => (DEFAULT_WINDOW, DEFAULT_INTERVAL)
      case Array(n)    => (n.toInt,        DEFAULT_INTERVAL)
      case _           => (DEFAULT_WINDOW, DEFAULT_INTERVAL)
    }
    (Seconds(w), Seconds(s))
  }

  /**
   * Set the window size, the number of streaming batch intervals for window functions,
   * "--window n[:m]". If the ":m" is specified, it is the number of batches to
   * jump for each window pass. It defaults to one.
   */
  def window(amount: Option[String] = Some(s"$DEFAULT_WINDOW:$DEFAULT_INTERVAL")): Opt = Opt(
    name   = "window",
    value  = amount map (_.toString),
    help   = s"-w | --win | --window n[:m]       Window size with optional delta.",
    parser = {
      case ("-w | --win" | "--window") +: nm +: tail => (("window", nm), tail)
    })

  /**
   * The checkpoint directory.
   */
  def checkpointDirectory(path: Option[String] = None): Opt = Opt(
    name   = "checkpoint-directory",
    value  = path,
    help   = s"""
       |  -cd | --checkpoint-dir | --checkpoint-directory  dir  The checkpoint directory.
       |                                                        """.stripMargin,
    parser = {
      case ("-cd" | "--checkpoint-dir" | "--checkpoint-directory") +: dir +: tail => (("checkpoint-directory", dir), tail)
    })

  /**
   * Should the checkpoint directory be deleted on *start*?
   * Use to start "clean" (default: true).
   */
  object DeleteCheckpointDirectory {
    def apply(value: Option[Boolean]): Opt = Opt(
      name   = "delete-checkpoint-directory",
      value  = value.map(_.toString),
      help   = s"""
         |  -dc | --delete-checkpoint-directory tf  Delete the checkpoint directory on start.
         |                                          """.stripMargin,
      parser = {
        case ("-dc" | "--delete-checkpoint-directory") +: tf +: tail => (("delete-checkpoint-directory", tf), tail)
      })
  }
}
