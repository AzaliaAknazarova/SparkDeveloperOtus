name := "homework_6"

version := "1"
scalaVersion := "2.13.16"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % Test
libraryDependencies += "com.github.mrpowers" %% "spark-fast-tests" % "1.1.0" % Test
