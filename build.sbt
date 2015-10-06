shellPrompt := { state =>
  "sbt (%s)> ".format(Project.extract(state).currentProject.id)
}

initialCommands += """
  import org.apache.spark.{SparkContext, SparkConf}
  import org.apache.spark.SparkContext._
  import org.apache.spark.sql.SQLContext
  val sparkConf = new SparkConf()
  sparkConf.setMaster("local[*]")
  sparkConf.setAppName("Spark Console")
  // Silence annoying warning from the Metrics system:
  sparkConf.set("spark.app.id", "Spark Console")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  """

cleanupCommands += """
  println("Closing the SparkContext:")
  sc.stop()
  """

addCommandAlias("ex2b",                       "runMain com.typesafe.training.sws.ex2.WordCountB")

addCommandAlias("ex2",                        "runMain com.typesafe.training.sws.ex2.WordCount")

addCommandAlias("ex4-crawl",                  "runMain com.typesafe.training.sws.ex4.Crawl")

addCommandAlias("ex4-ii",                     "runMain com.typesafe.training.sws.ex4.InvertedIndex")

addCommandAlias("ex5",                        "runMain com.typesafe.training.sws.ex5.NGrams")

addCommandAlias("ex6-joins",                  "runMain com.typesafe.training.sws.ex6.Joins")

addCommandAlias("ex6-shuffle",                "runMain com.typesafe.training.sws.ex6.DataShuffling")

addCommandAlias("ex7-df",                     "runMain com.typesafe.training.sws.ex7.SparkDataFrames")

addCommandAlias("ex7-sql",                    "runMain com.typesafe.training.sws.ex7.SparkSQL")

addCommandAlias("ex7-parquet",                "runMain com.typesafe.training.sws.ex7.SparkSQLParquet")

addCommandAlias("ex8-flights-backend",        "runMain com.typesafe.training.sws.ex8.flights.FlightsBackend")

addCommandAlias("ex8-flights-backend-socket", "runMain com.typesafe.training.sws.ex8.flights.FlightsBackend --socket localhost:10000")

addCommandAlias("ex8-flights-df",             "runMain com.typesafe.training.sws.ex8.flights.FlightsDataFrame")

addCommandAlias("ex8-flights-sql",            "runMain com.typesafe.training.sws.ex8.flights.FlightsSQL")

addCommandAlias("ex8-wiki-backend",           "runMain com.typesafe.training.sws.ex8.wikichanges.WikiChangesBackend")

addCommandAlias("ex8-predict-news",           "runMain com.typesafe.training.sws.ex8.wikichanges.PredictBreakingNews")

addCommandAlias("ex9-stations",               "runMain com.typesafe.training.sws.ex9.DivvyStations")

addCommandAlias("ex9-kmeans",                 "runMain com.typesafe.training.sws.ex9.DivvyKMeans")


fork := true
