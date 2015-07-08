# Typesafe Apache Spark Workshop Exercises

![Spark Logo](http://spark.apache.org/docs/latest/img/spark-logo-100x40px.png)

Copyright &copy; 2014-2015, Typesafe, All Rights Reserved.<br/>
Apache Spark and the Spark Logo are trademarks of [The Apache Software Foundation](http://www.apache.org/).

## Introduction

These exercises are part of the [Typesafe Apache Spark Workshop](http://typesafe.com/how/trainings), which teaches you why Spark is important for modern data-centric computing, and how to write Spark batch-mode, streaming, and SQL-based applications, and deployment options for Spark. We'll also introduce Spark modules for graph algorithms and machine learning, and we'll see how Spark can work as part of a larger _reactive_ application implemented with the [Typesafe Reactive Platform](http://typesafe.com/platform) (TRP).

## The Spark Version

This workshop uses Spark 1.3.1. It also uses Scala 2.11. Even though the Spark builds found at the [Apache site](http://spark.apache.org) are only for 2.10, in fact there are 2.11 artifacts in the Apache maven repositories.

> However, the 2.11 builds are considered experiment at this time. Consider using Scala 2.10 for production software.

The following documentation links provide more information about Spark:

* [Documentation](http://spark.apache.org/docs/latest/).
* [Scaladocs API](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package).

The [Documentation](http://spark.apache.org/docs/latest/) includes a getting-started guide and overviews. You'll find the [Scaladocs API](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package) useful for the workshop.

## Setup

You'll need to install `sbt`, which you can use for all the exercises, or to bootstrap Eclipse. Installing `sbt` isn't necessary if you use IntelliJ. See the [sbt website](http://www.scala-sbt.org/) for instructions on installing `sbt`.

You were given a download link to a zip file with the exercises. Unzip it in a convenient work directory and then select pick from the following subsections depending on how you want to work with the exercises:

### Working with sbt and a Text Editor

Open a terminal/console window and change to the working directory where you expanded the exercises. Run the `sbt` command, which puts you at the `sbt` prompt, then run the `test` "task", which downloads all dependencies, compiles the main code and the test code, and then runs the tests. It should finish with a *success* message. Here are the steps, where `$` is used as the "shell" or command prompt, `>` is the `sbt` prompt, and the `#...` are comments you shouldn't type in:

```
$ sbt
[info] ...                         # Information messages as sbt starts
> test
...
[success] Total time: ...          # Successfully compiled and tested the code.
```

Stay in `sbt` for the rest of the workshop. You'll run all your commands from here. Open the exercises in your favorite text editor.

### Working with Eclipse (Scala-IDE)

Since Eclipse plugins are "temperamental", we recommend downloading a complete Eclipse distribution with the Scala plugin installed from [http://scala-ide.org](). Also, we have found that the Scala 2.11.X version of the IDE has problems with the workshop project, so download the IDE for 2.10.X. However, this site also has the plugin URLs if you prefer trying that step first.

Unfortunately, a [ScalaTest](http://www.scalatest.org) plugin is not included, and it appears that the "incubator" version is obsolete that's hosted on the Scala-IDE update site, [http://download.scala-ide.org/sdk/helium/e38/scala210/stable/site](). Try the instructions on this [scalatest.org page](http://www.scalatest.org/user_guide/using_scalatest_with_eclipse) or use the `sbt` console to run the tests.

You'll need to generate Eclipse project files using `sbt`. (You can do this while Eclipse is doing its thing.) Open a terminal/console window and change to the working directory where you expanded the exercises. Run the following `sbt` command to generate the project files:

    sbt eclipse

It will take a few minutes, as it has to first download all the project dependencies.

Once it completes, start Eclipse and use the _File > Import_ menu option, then use the dialog to import the project you just created.

### Working with IntelliJ IDE.

Make sure you have installed the Scala plugin. If so, you can import the exercises as an SBT project.


## Going Forward from Here

There is also a [README](data/README.html) in the `data` directory. It describes the data we'll use for the exercises and where it came from.

To learn more, see the following:

* The Apache Spark [website](http://spark.apache.org/).
* The Apache Spark [documentation](http://spark.apache.org/docs/latest).
* The Apache Spark [Quick Start](http://spark.apache.org/docs/latest/quick-start.html). See also the examples in the [Spark distribution](https://github.com/apache/spark) and be sure to study the [Scaladoc](http://spark.apache.org/docs/latest/api.html) pages for key types such as `SparkContext, `SQLContext`, `RDD`, and `DataFrame`.
* Talks from Spark Summit [2013](http://spark-summit.org/2013), [2014](http://spark-summit.org/2014/), and [2015](http://spark-summit.org/2015/).

**Experience Reports:**

* [Spark at Twitter](http://www.slideshare.net/krishflix/seattle-spark-meetup-spark-at-twitter)

**Other Spark Based Libraries:**

* [Spark Notebook](https://github.com/andypetrella/spark-notebook) - An interactive, web-based environment similar to *ipython*.
* [Zeppelin](https://zeppelin.incubator.apache.org/) - Another notebook.
* [Snowplow's Spark Example Project](https://github.com/snowplow/spark-example-project).
* [Thunder - Large-scale neural data analysis with Spark](https://github.com/freeman-lab/thunder).

## For more about Typesafe:

* See [Typesafe Reactive Big Data](http://typesafe.com/reactive-big-data) to find other Activator templates.
* See [Typesafe Activator](http://typesafe.com/activator) to find other Activator templates.
* See [Typesafe](http://typesafe.com) for more information about our products and services.

