ThisBuild / scalaVersion := "2.12.18"

ThisBuild / javacOptions ++= Seq("--release", "8")

ThisBuild / scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-Xlint:-unused"
)

lazy val root = (project in file("."))
  .settings(
    name    := "lab02",
    version := "1.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.4.3" % "provided",
      "org.apache.spark" %% "spark-sql"  % "3.4.3" % "provided"
    )
  )
  .enablePlugins(AssemblyPlugin)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}

