// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

/**
 * Another example; play with the Crawl-generated data.
 * Assumes ex4/Crawl.scala has already been executed.
 */

// Our settings for sbt console and spark-shell both define the following for us:
// val sqlContext = new SQLContext(sc)

case class CrawlRecord(docid: String, contents: String)

def makeCrawlRecord(line: String): Option[CrawlRecord] = {
  val lineRE = """^\s*\(([^,]+),(.*)\)\s*$""".r
  line match {
    case lineRE(name, text) =>
      Some(CrawlRecord(name.trim, text.trim.toLowerCase))
    case badLine =>
      Console.err.println(s"Unexpected line: $badLine")
      None
  }
}

def dosql(query: String, n: Int = 100): Unit = {
  println(s"running query: $query")
  sqlContext.sql(query).take(n) foreach println
}

val crawlData = "golden/crawl/part-00000"

val crawlRDD = for {
  line <- sc.textFile(crawlData)
  cr <- makeCrawlRecord(line)
} yield cr

val crawl = sqlContext.createDataFrame(crawlRDD)
crawl.registerTempTable("crawl")
crawl.cache
crawl.printSchema

dosql("SELECT docid, contents FROM crawl limit 10")

// Create a second RDD with with the tokenized content,
// i.e., CrawlRecord(docid, word), reusing CrawlRecord.
val crawlPerWordRDD = crawlRDD flatMap {
  case CrawlRecord(docid, contents) =>
    contents.trim.split("""[^\w']""") map (word => CrawlRecord(docid, word))
}

val crawlPerWord = sqlContext.createDataFrame(crawlPerWordRDD)
crawlPerWord.registerTempTable("crawlPerWord")

dosql("SELECT * FROM crawlPerWord LIMIT 10")
dosql("SELECT DISTINCT * FROM crawlPerWord WHERE contents = 'management'")
dosql("""
  SELECT contents, COUNT(*) AS c FROM crawlPerWord
  GROUP BY contents
  ORDER BY c DESC LIMIT 100""")
