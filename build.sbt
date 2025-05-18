ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.5"

lazy val homework_2 = project.in(file("homework_2"))
lazy val homework_3 = project.in(file("homework_3"))
lazy val homework_4 = project.in(file("homework_4"))
lazy val homework_5 = project.in(file("homework_5"))
lazy val homework_6 = project.in(file("homework_6"))

lazy val root = (project in file("."))
  .aggregate(homework_2)
  .aggregate(homework_3)
  .aggregate(homework_4)
  .aggregate(homework_5)
  .aggregate(homework_6)
  .settings(
    name := "SparkDeveloper"
  )