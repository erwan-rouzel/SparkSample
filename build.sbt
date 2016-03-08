lazy val commonSettings = Seq(
    organization := "fr.ecp.sio",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "2.10.4"
)

lazy val root = (project in file(".")).
    settings(commonSettings: _*).
    settings(
        libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0"
)
