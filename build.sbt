name := "alpakka-kafka-commit-failed-scenario"

version := "0.1"

scalaVersion := "2.13.3"

val AkkaVersion = "2.5.31"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-kafka" % "2.0.5",
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion
)
