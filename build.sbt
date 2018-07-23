name := "SparkStreaming"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "1.5.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % sparkVersion

//Spark Streaming Integration only compatible with Spark version 1.5.2