name := "WeblogChallenge"

version := "1.0"

scalaVersion := "2.11.7"
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
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "org.apache.spark" %% "spark-hive" % "2.0.0",
  "org.apache.spark" %% "spark-streaming" % "2.0.0",
  "org.apache.spark" %% "spark-streaming-flume" % "2.0.0",
  "org.apache.spark" %% "spark-mllib" % "2.0.0",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.databricks" % "spark-csv_2.10" % "1.4.0"
)
