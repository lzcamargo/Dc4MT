name := "mt-scala"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.3"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.3"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.4.3"
libraryDependencies += "com.databricks" % "com.databricks.spark.xml._2.12" % "2.4.3"


// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-core
//libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.10.3"

libraryDependencies += "guru.nidi" % "graphviz-java" % "0.12.1"


resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven/"
libraryDependencies += "graphframes" % "graphframes" % "0.6.0-spark2.3-s_2.11"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("git.properties", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
