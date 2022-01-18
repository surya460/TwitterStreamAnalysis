name := "TwitterStreamAnalysis"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0-preview" % "compile"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.0-preview" % "compile"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0-preview" % "compile"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.4"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.4"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"
