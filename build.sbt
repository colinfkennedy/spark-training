shellPrompt := { state =>
  "sbt (%s)> ".format(Project.extract(state).currentProject.id)
}

initialCommands += """
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark.sql.SQLContext
  val sc = new SparkContext("local[*]", "Spark Console")
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  """

cleanupCommands += """
  println("Closing the SparkContext:")
  sc.stop()
  """

addCommandAlias("ex2a",         "runMain com.typesafe.training.sws.ex2.WordCountA")

addCommandAlias("ex2",          "runMain com.typesafe.training.sws.ex2.WordCount")

addCommandAlias("ex3",          "runMain com.typesafe.training.sws.ex3.MatrixAddition")

addCommandAlias("ex4-crawl",    "runMain com.typesafe.training.sws.ex4.Crawl")

addCommandAlias("ex4-ii",       "runMain com.typesafe.training.sws.ex4.InvertedIndex")

addCommandAlias("ex5",          "runMain com.typesafe.training.sws.ex5.NGrams")

addCommandAlias("ex6-joins",    "runMain com.typesafe.training.sws.ex6.Joins")

addCommandAlias("ex6-shuffle",  "runMain com.typesafe.training.sws.ex6.DataShuffling")

addCommandAlias("ex7-df",       "runMain com.typesafe.training.sws.ex7.SparkDataFrames")

addCommandAlias("ex7-sql",      "runMain com.typesafe.training.sws.ex7.SparkSQL")

addCommandAlias("ex7-parquet",  "runMain com.typesafe.training.sws.ex7.SparkSQLParquet")

addCommandAlias("ex8-backend",  "runMain com.typesafe.training.sws.ex8.SparkStreamingBackend")

addCommandAlias("ex8-backend-socket",  "runMain com.typesafe.training.sws.ex8.SparkStreamingBackend --socket localhost:10000")

addCommandAlias("ex8-df",       "runMain com.typesafe.training.sws.ex8.SparkStreamingDataFrame")

addCommandAlias("ex8-sql",      "runMain com.typesafe.training.sws.ex8.SparkStreamingSQL")

addCommandAlias("wiki-backend", "runMain com.typesafe.training.sws.ex8.wikichanges.WikiChangesBackend")

addCommandAlias("predict-news", "runMain com.typesafe.training.sws.ex8.wikichanges.PredictBreakingNews")


fork := true
