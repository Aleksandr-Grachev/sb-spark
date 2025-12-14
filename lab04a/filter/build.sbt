ThisBuild / scalaVersion := "2.12.18"

ThisBuild / javacOptions ++= Seq("--release", "8")

ThisBuild / scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-Xlint:-unused"
)

// # Информация о кластере

// * Версия Spark `3.4.3`
// * Версия sbt `1.6.2`
// * Адрес PostgreSQL (с портом): `10.0.0.31:5432`
// * Версия библиотеки PostgreSQL (зависимости): `42.7.3` или последняя
// * Адрес Cassandra: `10.0.0.31`, порт: `9042`
// * Версия Cassandra: `3.5.1`
// * Версия Joda-Time: `2.12.7`
// * Адрес Elasticsearch REST API: `10.0.0.31`, порт: `9200`
// * Версия Elasticsearch: `8.14.2`
// * Адрес Kafka bootstrap server (с портом): `spark-master-1:6667`
// * Путь к kafka скриптам: `/usr/hdp/current/kafka-broker/bin/`
// * Адрес Zookeper (с портом): `spark-node-1.newprolab.com:2181`, либо `spark-master-1`, либо если он установлен на ноде (spark-master-2/3), то можно просто `localhost`
// * Адрес Kibana Web UI, REST API: `10.0.0.31`, порт: `5601`

lazy val root = (project in file("."))
  .settings(
    name    := "filter",
    version := "1.0",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"           % "3.4.3"  % "provided",
      "org.apache.spark" %% "spark-sql"            % "3.4.3"  % "provided",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.3"  % "provided",
      "joda-time"         % "joda-time"            % "2.12.7" % "provided"
    )
  )
  .enablePlugins(AssemblyPlugin)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
