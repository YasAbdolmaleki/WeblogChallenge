name := "WeblogChallenge"

version := "1.0"

scalaVersion := "2.11.8"
/*
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.0.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.0"
libraryDependencies += "joda-time" % "joda-time" % "2.9.4"
libraryDependencies += "log4j" % "log4j" % "1.2.17"
libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.4.0"
*/

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.3.1",
  "org.apache.spark" %% "spark-hive" % "1.3.1",
  "org.apache.spark" %% "spark-streaming" % "1.3.1",
  "org.apache.spark" %% "spark-streaming-flume" % "1.3.1",
  "org.apache.spark" %% "spark-mllib" % "1.3.1",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)
