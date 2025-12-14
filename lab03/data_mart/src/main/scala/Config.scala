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

object Config {
  def newprolab =
    new Config(
      cassandra = CassandraCfg(
        host = "10.0.0.31",
        port = 9042
      ),
      es = ElasticSearchCfg(host = "10.0.0.31", port = 9200),
      pg = PGCfg(
        host = "10.0.0.31",
        port = 5432,
        user = "aleksandr_grachev",
        password = "nGA73sOE",
        in = PgBdCfg(
          db = "labdata"
        ),
        out = PgBdCfg(
          db = "aleksandr_grachev"
        )
      )
    )
}

case class CassandraCfg(
    host: String,
    port: Int
)

case class ElasticSearchCfg(
    host: String,
    port: Int
)

case class PgBdCfg(
    db: String
)

case class PGCfg(
    host: String,
    port: Int,
    user: String,
    password: String,
    in: PgBdCfg,
    out: PgBdCfg
)

case class Config(
    cassandra: CassandraCfg,
    es: ElasticSearchCfg,
    pg: PGCfg
)
