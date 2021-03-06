name := "WordCount"

version := "0.1"

scalaVersion := "2.12.8"

resolvers  += "MavenRepository" at "https://mvnrepository.com"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test