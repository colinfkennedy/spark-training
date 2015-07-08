// Do word count on the Enron emails in a way that creates 200 partitions.
val inputHam  = sc.textFile("data/enron-spam-ham/ham100")
val inputSpam = sc.textFile("data/enron-spam-ham/spam100")
val wc = inputHam.union(inputSpam).
  map(_.toLowerCase).
  flatMap(_.split("""\W+""")).
  map((_,1)).
  reduceByKey(_+_)
wc.saveAsTextFile("output/wc-local-star")
