// A Scala script we will use interactively in the Spark Shell.

// If you're using the Spark Shell or our build process, the following
// statements are invoked automatically as the interpreter starts.
// They "import" library types, construct the SparkContext "sc" and
// SQLContext "sqlContext" (which we'll learn about later), and import some
// special types and methods from an "sqlContext.implicits" object.
// Otherwise, our scripts would need to include these statements explicitly:
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
// import org.apache.spark.sql.SQLContext
// val sc = new SparkContext("local", "Console")
// val sqlContext = new SQLContext(sc)
// import sqlContext.implicits._

// Load the plays of Shakespeare, then convert each line to lower case.
// Both input and lines will be RDDs.
val input = sc.textFile("data/all-shakespeare.txt")
val lines = input.map(line => line.toLowerCase)

// Cache the data in memory for faster, repeated retrieval.
lines.cache

// Find all verses that mention "hamlet".
// Note the shorthand for the function literal:
// _.contains("hamlet") behaves the same as line => line.contains("hamlet").
val hamlet = lines.filter(_.contains("hamlet"))

// The () are optional in Scala for no-argument methods
val count = hamlet.count()       // How many occurrences of hamlet?
val array = hamlet.collect()     // Convert the RDD into a collection (array)
array.take(20) foreach println   // Take the first 20, and print them 1/line.
hamlet.take(20) foreach println  // ... but you don't have to "collect" first.

// Create a separate filter function instead and pass it as an argument to the
// filter method. "filterFunc" is a value that's a function of type
// String to Boolean.
val filterFunc: String => Boolean =
    s => s.contains("claudius") || s.contains("gertrude")
// Equivalence, due to type inference:
//  s => s.contains("claudius") || s.contains("gertrude")
//  (s:String) => s.contains("claudius") || s.contains("gertrude")

// Filter the hamlets for the verses that mention God or Christ (lowercase)
val hamletPlusClaudiusOrGertrude  = hamlet filter filterFunc
// Count how many we found. (Note we dropped the parentheses after "count")
val count2 = hamletPlusClaudiusOrGertrude.count
hamletPlusClaudiusOrGertrude foreach println

// This *method* creates a filter function using the two input strings:
def makeAndFilter(s1: String, s2: String): String => Boolean =
    s => s.contains(s1) && s.contains(s2)

val rottenDenmark = lines filter (makeAndFilter("rotten", "denmark"))
val countGoodEvil = rottenDenmark.count
rottenDenmark foreach println

// A few other useful tools:
rottenDenmark.toDebugString

// Clean up nicely. You don't need to do, because we set up SBT "cleanupCommands"
// to stop the SparkContext when we exit the console.
// sc.stop
