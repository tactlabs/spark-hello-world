#!/bin/bash

echo "Compiling and assembling application..."
sbt assembly

# Directory where spark-submit is defined
# Install spark from https://spark.apache.org/downloads.html
# NOTE: you need to change this to your spark installation
# SPARK_HOME=/home/${USER}/spark-3.1.2-bin-hadoop3.2
SPARK_HOME=/usr/local/Cellar/apache-spark/3.2.1/libexec

# JAR containing a simple hello world
JARFILE=`pwd`/target/scala-2.12/HelloWorld-assembly-0.1.0.jar

# Run it locally
${SPARK_HOME}/bin/spark-submit --class HelloWorld --master local $JARFILE --conf "spark.executor.extraClassPath=/Users/str-kwml0020/projects/postgresql-42.3.0.jar" --conf "spark.driver.extraClassPath=/Users/str-kwml0020/projects/postgresql-42.3.0.jar"
