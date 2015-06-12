name := """apium"""

organization := "com.lvxingpai"

version := "0.1"

crossScalaVersions := Seq("2.10.4", "2.11.4")

scalacOptions ++= Seq("-feature", "-deprecation")

libraryDependencies ++= Seq(
  // Change this to another test framework if you prefer
  "org.scalatest" %% "scalatest" % "2.1.6" % "test",
  "com.typesafe.akka" %% "akka-actor" % "2.3.5",
  "com.thenewmotion.akka" %% "akka-rabbitmq" % "1.2.4",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.5.0",
  "org.joda" % "joda-convert" % "1.7",
  "joda-time" % "joda-time" % "2.8"
)

publishTo := {
  val nexus = "http://nexus.lvxingpai.com/content/repositories/"
  if (isSnapshot.value)
    Some("publishSnapshots" at nexus + "snapshots")
  else
    Some("publishReleases"  at nexus + "releases")
}
