
name := "FindDS"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.5" % "provided",
  "com.databricks" %% "spark-csv" % "1.2.0" )


//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"

//libraryDependencies += "com.databricks" %% "spark-csv" % "1.2.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}