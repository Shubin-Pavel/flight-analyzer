name := "flight-analyzer"

version := "0.1"

scalaVersion := "2.12.18"

idePackagePrefix := Option("com.example")

lazy val sparkVersion = "3.5.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
)
