name := "sparkHadoop"
assemblyJarName in assembly := "sparkqsql.jar"
version := "0.1"
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
scalaVersion := "2.10.5"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.1"
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.5.0"
libraryDependencies += "commons-net" % "commons-net" % "3.6"
libraryDependencies += "com.typesafe" % "config" % "1.3.2"