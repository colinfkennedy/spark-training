import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import com.typesafe.training.data.Verse

/**
 * Example of Accessing Hive Tables directly and using HiveQL, its dialect
 * of SQL. This script Writes query results to the console, rather than to a file.
 * Using a script facilitates interactive extensions and exploration.
 *
 * See https://cwiki.apache.org/confluence/display/Hive/LanguageManual
 * for details on HiveQL.
 */

// The analog of SQLContext we used in the previous exercise is Hive context
// that starts up an instance of Hive where metadata is stored locally in
// an in-process "metastore", which is stored in the ./metadata directory.
// Similarly, the ./warehouse directory is used for the regular data
// "warehouse". HiveContext looks for data about the metastore (host, port, etc.)
// by reading the local hive-site.xml.
val hiveContext = new HiveContext(sc)
import hiveContext.implicits_   // Make methods local, as for SQLContext

// Determine the user name. Used in DDL statements.
val user = sys.env.get("USER") match {
  case Some(user) => user
  case None =>
    println("ERROR: USER environment variable isn't defined. Using root!")
    "root"
}

def print[T](msg: String, rdd: RDD[T], n: Int = 100): Unit = {
  println(s"$msg: (size = ${rdd.count})")
  rdd.take(n) foreach println
}

// The "hql" method let's us run the full set of Hive SQL statements.

// Let's create a database for our work:
println("Create and use a work database:")
hql("CREATE DATABASE work")
hql("USE work")

// Let's create a table for our KJV data. Note that Hive lets us specify
// the field separator for the data and we can make a table "external",
// which means that Hive won't "own" the data, but instead just read it
// from the location we specify.
//
// NOTES - We won't actually run this script because Hadoop is required.
// There are a also several API quirks:
// 1. You must have Hadoop installed and a Spark build for your distribution.
//    You can download prebuilt variants at http://spark.apache.org. See also
//    this discussion: http://spark.apache.org/docs/latest/sql-programming-guide.html
// 2. This example uses the King James Version of the Bible as data.
//    You must first copy the data file "data/kjvdat.csv" to an empty
//    "/user/$user/data" directory, where $user is your Hadoop user
//    name. Hive expects the LOCATION used below to be an absolute
//    directory path and it will read all the files in it.
// 3. Don't include a trailing "/" in the LOCATION path. It confuses Hive.
// 4. Omit semicolons at the end of the HQL (Hive SQL) strings. They confuse
//    the API, however if you use Hive's own REPL, you must include them.
// 5. The query results are return in an RDD, so you can do any postprocessing
//    steps that we've done before. To dump the results, use the print() method
//    defined above.

println("Create the 'external' kjv Hive table:")
hql(s"""
  CREATE EXTERNAL TABLE IF NOT EXISTS kjv (
    book    STRING,
    chapter INT,
    verse   INT,
    text    STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  LOCATION '/user/$user/data/kjv'
  """)

print("How many records?: COUNT(*)",
  hql("SELECT COUNT(*) FROM kjv"))

print("Print the first few records: LIMIT 10",
  hql("SELECT * FROM kjv LIMIT 10"))

val by_book = hql("""
  SELECT * FROM (
    SELECT book, COUNT(*) AS count FROM kjv GROUP BY book) bc
  WHERE bc.book != ''
  """))

print("Run a GROUP BY book query", by_book)

print("SELECT verses with 'God'",
  hql("SELECT * FROM kjv WHERE text LIKE '%God%'"))

// Drop the table and database. We're using Hive's embedded Derby SQL "database"
// for the "metastore" (Table metadata, etc.) See the "metastore" subdirectory
// you now have! Because the table is EXTERNAL, we only delete the metadata, but
// not the table data itself.
print("Drop the table.", hql("DROP TABLE kjv"))
print("Drop the database.", hql"DROP DATABASE work")

// You would do this in your scripts, but not if running in SBT, as it does it
// for you when you quit the console.
// sc.stop()

// For further experimentation:
//  1. See https://cwiki.apache.org/confluence/display/Hive/LanguageManual
//     for details on HiveQL.
//  2. Sort the output by the words. Hive has an ORDER BY and a SORT BY. How
//     do they differ?
//  2. Try a JOIN with the "data/abbrevs_to_names.tsv" data to convert the
//     book abbreviations to full titles. Follow the example above to create an
//     EXTERNAL TABLE. Note: Use a separate directory!
//  4. Play with the SchemaRDD DSL.
//  5. Try some of the other sacred text data files.
