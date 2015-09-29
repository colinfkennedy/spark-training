// First implementation of Word Count, written as a Spark script.

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

// Load Shakespeare's plays creating an RDD. Then convert each line to lower case.
val input = sc.textFile("data/all-shakespeare.txt")
val lines = input.map(line => line.toLowerCase)

// Split each line into words on non-alphanumeric sequences of
// characters. Since each single line is converted to a sequence of
// words, we use flatMap to flatten the sequence of sequences into a
// single sequence of words.
// Then use groupBy to bring together all words that are the same. This returns
// a "record" type of (word, sequence(word, word, word, ...)) for each unique
// word (you'll notice some duplication in the data here; we'll fix that later.)
// Finally, mapValues lets us transform the "value" part of the (word, seq) pairs,
// where the "key" part is the word. In this case, we simply return the size of
// the sequence, effectively counting the number of occurrences of the
// corresponding word.
val wordCount = lines.
  flatMap(line => line.split("""\W+""")).
  groupBy(word => word).
  mapValues(seq => seq.size)

println(s"There are ${wordCount.count} unique words")

wordCount.take(100).foreach(println)
