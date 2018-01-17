name := "NYC Trip Prediction - BDAD Final Project"

version := "0.4"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.1.0",
  "com.databricks" %% "spark-csv" % "1.5.0"
)
