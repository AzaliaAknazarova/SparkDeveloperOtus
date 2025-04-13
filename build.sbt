ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.5"

lazy val homework_2 = project.in(file("homework_2"))

lazy val root = (project in file("."))
  .aggregate(homework_2)
  .settings(
    name := "SparkDeveloper"
  )
