name := "mot-data-in-spark"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % "2.10.6",
  "log4j" % "log4j" % "1.2.14",
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-mllib" % "1.6.0",
  "org.json4s" %% "json4s-jackson" % "3.2.7",
  "com.github.nscala-time" %% "nscala-time" % "2.12.0",
  "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3",
  "org.scalatest" %% "scalatest" % "2.2.0" % "test"
)