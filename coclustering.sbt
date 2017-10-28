name := "coclustering project"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
 "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
 "org.apache.spark" %% "spark-mllib" % "1.6.0"  //,
// "com.databricks" %% "spark-csv" % "2.10"
)
