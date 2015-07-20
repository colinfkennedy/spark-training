package com.typesafe.training.data

/** Represent Stations for the Divvy (Chicago bike sharing) data set. */
case class Station(
  id:         Int,
  name:       String,
  latitude:   Double,
  longitude:  Double,
  capacity:   Int,
  landmark:   Int,
  onlineDate: YMD)

object Station {
  /**
   * Parse a record string into a `Station` instance, but return an `Option`
   * so that on error, a `None` is returned. Handle the case when the first line
   * is the column names.
   * @todo Consider using the spark-csv package instead (http://spark-packages.org/package/databricks/spark-csv).
   */
  def parse(line: String): Option[Station] =
      if (line.trim.startsWith("id")) None else doparse(line)

  private def doparse(line: String): Option[Station] = try {
    line.trim.split("""\s*,\s*""") match {
      case Array(id, name, lat, long, cap, landmark, date) =>
        YMD.parse(date) match {
          case Some(ymd) => Some(Station(id.toInt, name, lat.toDouble, long.toDouble, cap.toInt, landmark.toInt, ymd))
          case _ =>
            Console.err.println(s"Bad line: $line")
            None
        }
      case _ =>
        Console.err.println(s"Bad line: $line")
        None
    }
  } catch {
    case nfe: NumberFormatException =>
      Console.err.println(s"At least one integer or double field string failed to parse. line: $line")
      None
  }
}

/** Represent Trips for the Divvy (Chicago bike sharing) data set. */
case class Trip(
  tripId:          Int,
  startTime:       Date,
  stopTime:        Date,
  bikeId:          Int,
  tripDuration:    Int,
  fromStationId:   Int,
  fromStationName: String,
  toStationId:     Int,
  toStationName:   String,
  userType:        String,
  gender:          String,
  birthday:        String)

object Trip {
  /**
   * Keep this value consistent with `Trip`. It is passed to String#split().
   * Why add the number of expected fields? Because String#split() drops
   * trailing whitespace otherwise! I.e.,
   * ",x,".split(",") returns Array("","x"), but
   * ",x,".split(",", 3) returns Array("","x", "").
   * For this data set, the last two fields, gender and birthday, are blank.
   */
  val NUMBER_TRIP_FIELDS = 12

  /**
   * Parse a record string into a `Trip` instance, but return an `Option`
   * so that on error, a `None` is returned. Handle the case when the first line
   * is the column names.
   * @todo Consider using the spark-csv package instead (http://spark-packages.org/package/databricks/spark-csv).
   */
  def parse(line: String): Option[Trip] =
    if (line.trim.startsWith("trip")) None else doparse(line)

  private def doparse(line: String): Option[Trip] = try {
    line.trim.split("""\s*,\s*""", 12) match {
      case Array(tripId, start, stop, bid, td, fid, fn, tid, tn, user, g, bd) =>
        (Date.parse(start), Date.parse(stop)) match {
          case (Some(startDate),Some(stopDate)) =>
            Some(Trip(tripId.toInt, startDate, stopDate, bid.toInt, td.toInt, fid.toInt, fn, tid.toInt, tn, user, g, bd))
          case _ =>
            Console.err.println(s"Bad line: $line")
            None
        }
      case _ =>
        Console.err.println(s"Bad line: $line")
        None
    }
  } catch {
    case nfe: NumberFormatException =>
      Console.err.println(s"At least one integer field string failed to parse. line: $line")
      None
  }
}
