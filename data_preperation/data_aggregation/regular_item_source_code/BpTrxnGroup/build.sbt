name := "BpTrxnGroup"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided", // spark runtime already provides jars
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  // not relevant, just allows me to pass command line options to spark job
  "args4j" % "args4j" % "2.33",
  "com.bizo" % "args4j-helpers_2.10" % "1.0.0"
)
