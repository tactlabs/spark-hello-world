name := "HelloWorld"
version := "0.1.0"
scalaVersion := "2.12.15"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided"

// https://mvnrepository.com/artifact/org.postgresql/postgresql
libraryDependencies += "org.postgresql" % "postgresql" % "42.3.0"
