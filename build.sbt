name := "spark-rabbitmq-client"

version := "0.1"

scalaVersion := "2.12.17"

idePackagePrefix := Some("eu.navispeed.rabbitmq")

libraryDependencies += "com.rabbitmq" % "amqp-client" % "5.16.0"
val sparkVersion = "3.1.3"



libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"

libraryDependencies += "net.liftweb" %% "lift-json" % "3.5.0"


libraryDependencies += "org.projectlombok" % "lombok" % "1.18.24" % "provided"



libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % "test"

