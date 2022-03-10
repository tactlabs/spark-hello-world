## spark-hello-world 

A simple hello world using Apache Spark

## Setup

Install [Apache Spark](https://spark.apache.org/downloads.html) and [SBT](http://www.scala-sbt.org/release/tutorial/Setup.html) first.

Creating jar file
```
sbt assembly
```

Run with Spark
```
sh submit-spark-hello-world.sh
```

In `submit-spark.hello-world.sh`, set `SPARK_HOME` pointing to the above spark installation.


