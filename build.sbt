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

addCommandAlias("ex2a", "runMain com.typesafe.training.sws.ex2.WordCountA")

addCommandAlias("ex2",  "runMain com.typesafe.training.sws.ex2.WordCount")

addCommandAlias("ex3",  "runMain com.typesafe.training.sws.ex3.MatrixAddition")

addCommandAlias("ex4a", "runMain com.typesafe.training.sws.ex4.Crawl")

addCommandAlias("ex4b", "runMain com.typesafe.training.sws.ex4.InvertedIndex")

addCommandAlias("ex5",  "runMain com.typesafe.training.sws.ex5.NGrams")

addCommandAlias("ex6a",  "runMain com.typesafe.training.sws.ex6.Joins")

addCommandAlias("ex6b",  "runMain com.typesafe.training.sws.ex6.DataShuffling")

addCommandAlias("ex7a", "runMain com.typesafe.training.sws.ex7.SparkDataFrames")

addCommandAlias("ex7b", "runMain com.typesafe.training.sws.ex7.SparkSQL")

addCommandAlias("ex7c", "runMain com.typesafe.training.sws.ex7.SparkSQLParquet")

addCommandAlias("ex8a", "runMain com.typesafe.training.sws.ex8.SparkStreaming")

addCommandAlias("ex8b", "runMain com.typesafe.training.sws.ex8.SparkStreamingSQL")

addCommandAlias("wiki-backend", "runMain com.typesafe.training.sws.ex8.wikichanges.WikiChangesBackend")

addCommandAlias("predict-news", "runMain com.typesafe.training.sws.ex8.wikichanges.PredictBreakingNews")

// Use this one to run the driver for the SparkStreaming example:
addCommandAlias("ex8main",  "runMain com.typesafe.training.sws.ex8.SparkStreamingMain")


fork := true
