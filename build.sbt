name := "com.inkenkun.x1.kafka.practice"

version := "1.0"

scalaVersion := "2.11.7"

scalacOptions ++= Seq( "-deprecation", "-encoding", "UTF-8" )

resolvers ++= Seq(
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases"
)

libraryDependencies ++= Seq(
  "org.apache.kafka"  % "kafka_2.11"     % "0.9.0.1",
  "com.typesafe"      % "config"         % "1.3.0",
  "org.scalatest"     % "scalatest_2.11" % "2.2.4" % "test"
)