name := """apium"""

organization := "com.lvxingpai"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  // Change this to another test framework if you prefer
  "org.scalatest" %% "scalatest" % "2.1.6" % "test",
  // Akka
  "com.typesafe.akka" %% "akka-actor" % "2.3.5",
  //"com.typesafe.akka" %% "akka-remote" % "2.3.5",
  //"com.typesafe.akka" %% "akka-testkit" % "2.3.5"
  "com.thenewmotion.akka" %% "akka-rabbitmq" % "1.2.4",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.5.0",
  "joda-time" % "joda-time" % "2.7"
)

publishTo := {
  val nexus = "http://nexus.lvxingpai.com/content/repositories/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "snapshots")
  else
    Some("releases"  at nexus + "releases")
}
