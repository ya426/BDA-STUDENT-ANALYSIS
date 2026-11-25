ThisBuild / scalaVersion := "2.12.18"
name := "student-academic-analysis"
version := "1.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.3"
Compile / run / fork := true
Compile / run / javaOptions += "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
