package com.typesafe.training.util

import java.util.Date
import java.text.SimpleDateFormat

object Timestamp {
  val fmt = new SimpleDateFormat ("yyyy.MM.dd-kk.mm.ss");

  // Simple hack for testing. Not threadsafe, of course...
  var isTest: Boolean = false

  def now(): String =
    if (isTest) ""
    else fmt.format(new Date())
}
