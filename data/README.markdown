# README for the data directory

## Airline Flight Data

This data for is a sample of a multi-GB data set for all flights in the US and territories from 1987 to 2008. It includes information such as the scheduled and actual flight times, delays and reasons for them, the air carriers and planes, etc. Curiously, it appears that the data for December is truncated in most of the files (one per year).

The data was retrieved from this web site, [http://stat-computing.org/dataexpo/2009/the-data.html](), sourced from this government site, [http://www.transtats.bts.gov/OT_Delay/OT_DelayCause1.asp]().

Because of its size, only a subset of the flight records are included here, for Alaskan Airlines in 2008. You might download the full data files form the link above.

The data is found in the folder `data/airline-flights`. Here is the directory organization:

| File | Description
| :--- | :----------
`airports.csv` | CSV file of airport IATA codes (e.g., ORD), name (e.g., O'Hare), city, state, country, latitude, and longitude.
`carriers.csv` | CSV file of carrier codes (e.g., UA) and names (e.g., United Airlines).
`plane-data.csv` | CSV file of data on individual planes.
`alaska-airlines/2008.csv` | CSV file for Alaska's flight data for 2008.

There are Scala "case classes" that define the schemas for each of these data sets and also include parsers. See `src/main/scala/com/typesafe/training/data`.

## Shakespeare's Plays

The plain-text version of all of Shakespeare's plays, formatted exactly as you typically see them printed, i.e., using the conventional spacing and layout for plays.

| Directory | Description
| :--- | :----------
`data/all-shakespeare.txt` | The folio of Shakespeare's plays, as plain text.

## Sacred Texts

The following ancient, sacred texts are from [www.sacred-texts.com/bib/osrc/](http://www.sacred-texts.com/bib/osrc/). All are copyright-free texts, where each verse is on a separate line, prefixed by the book name, chapter, number, and verse number, all "|" separated.

| File | Description
| :--- | :----------
`kjvdat.txt` | The King James Version of the Bible. For some reason, each line (one per verse) ends with a "~" character.
`t3utf.dat` | Tanach, the Hebrew Bible.
`vuldat.txt` | The Latin Vulgate.
`sept.txt` | The Septuagint (Koine Greek of the Hebrew Old Testament).
`ugntdat.txt` | The Greek New Testament.
`apodat.txt` | The Apocrypha (in English).
`abbrevs-to-names.tsv` | A map from the book abbreviations used in these texts to the full book names. Derived using data from the sacred-texts.com site.

There are many other texts from the world's religious traditions at the [www.sacred-texts.com](http://www.sacred-texts.com) site, but most of the others aren't formatted into one convenient file like these examples.

## Classics

[http://classics.mit.edu/]() is a great source of ancient texts. *Caesar's Gallic Wars* is including here, `classics/gallic.mb.txt`, which describes his conquest of Gaul (roughly modern France) between 58 and 50 BC.

## Email Classified as SPAM and HAM

A sample of SPAM/HAM classified emails from the well-known Enron email data set was adapted from [this research project](http://www.aueb.gr/users/ion/data/enron-spam/). Each file is plain text, partially formatted (i.e., with `name:value` headers) as used in email servers and clients.

| Directory | Description
| :--- | :----------
`enron-spam-hamham100` | A sample of 100 emails from the dataset that were classified as HAM.
`enron-spam-hamspam100` | A sample of 100 emails from the dataset that were classified as SPAM.

## Divvy Bike Data

Divvy runs a bike-sharing service in Chicago. They recently posted a snapshot of their anonymized data online as part of a data science challenge, [https://www.divvybikes.com/datachallenge](). This page has some interesting visualizations done by participants. The list of latitudes and longitudes for the stations as of 2013 is included here, `Divvy/stations-lat-long.csv`. The full data set, including records of trips for 2013, is available from the web site.

