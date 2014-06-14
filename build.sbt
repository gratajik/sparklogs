import AssemblyKeys._

name := "sparklogs"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.3"

libraryDependencies += "com.jcraft" % "jsch" % "0.1.43-1"

assemblySettings

jarName in assembly := "sparklogs.jar"

mainClass in assembly := Some("SparkLogs")
