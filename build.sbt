ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "arquet",

    libraryDependencies ++= Seq(
      // For testing
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      // For binary encoding/decoding (helpful but optional to start)
      "org.scodec" %% "scodec-core" % "2.2.0",
      "org.scodec" %% "scodec-bits" % "1.1.38"
    )
  )