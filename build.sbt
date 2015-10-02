name := """kanbench"""

version := "1.0"

scalaVersion := "2.11.6"

scalacOptions += "-deprecation"

resolvers ++= Seq(
  "Sonatype OSS Releases"  at "http://oss.sonatype.org/content/repositories/releases/",
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
  )

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.11",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.11" % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "junit" % "junit" % "4.11" % "test",
  "com.novocode" % "junit-interface" % "0.10" % "test",
  "com.github.tototoshi" %% "scala-csv" % "1.1.2",
  "org.apache.commons" % "commons-math3" % "3.4.1",
  "com.amazonaws" % "aws-java-sdk" % "1.9.16",
  "org.scala-lang" % "scala-reflect" % "2.11.5",
  "net.ceedubs" %% "ficus" % "1.1.2",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.2",
  "com.github.scopt" %% "scopt" % "3.3.0",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0-cdh5.4.7",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0-cdh5.4.7",
  "org.apache.hadoop" % "hadoop-annotations" % "2.6.0-cdh5.4.7",
  "org.apache.hbase" % "hbase" % "1.0.0-cdh5.4.7",
  "org.apache.hbase" % "hbase-client" % "1.0.0-cdh5.4.7",
  "org.apache.hbase" % "hbase-common" % "1.0.0-cdh5.4.7"
)

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")

