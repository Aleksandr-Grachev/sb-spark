import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object RawData {

  /**   Юзеры из Кассандры
   *    - uid` – уникальный идентификатор пользователя, string
    *   - `gender` – пол пользователя, F или M - string
    *   - `age` – возраст пользователя в годах, integer
    */
  def readClients(spark: SparkSession): DataFrame =
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "clients", "keyspace" -> "labdata"))
      .load()

  /** Из бэкенда интернет-магазина приходят отфильтрованные и обогащенные
    * сообщения о просмотрах страниц товаров и покупках. Сообщения хранятся в
    * Elasticsearch в формате json в следующем виде:
    *
    *   - `uid` – уникальный идентификатор пользователя, тот же, что и в базе с
    *     информацией о клиенте (в Cassandra), либо null, если в базе
    *     пользователей нет информации об этих посетителях магазина, string
    *   - `event_type` – buy или view, соответственно покупка или просмотр
    *     товара, string
    *   - `category` – категория товаров в магазине, string
    *   - `item_id` – идентификатор товара, состоящий из категории и номера
    *     товара в категории, string
    *   - `item_price` – цена товара, integer
    *   - `timestamp` – unix epoch timestamp в миллисекундах
    */
  def readShop(spark: SparkSession)(es: ElasticSearchCfg): DataFrame =
    spark.read
      .format("org.elasticsearch.spark.sql")
      .options(
        Map(
          "es.read.metadata"  -> "true",
          "es.nodes.wan.only" -> "true",
          "es.nodes"          -> es.host,
          "es.port"           -> es.port.toString(),
          "es.net.ssl"        -> "false"
        )
      )
      .load("visits")

  /**   - `uid` – уникальный идентификатор пользователя, тот же, что и в базе с
    *     информацией о клиенте (в Cassandra),
    *   - `visits` c некоторым числом пар (timestamp, url), где `timestamp` –
    *     unix epoch timestamp в миллисекундах, `url` - строка.
    */
  def readWeblogs(spark: SparkSession): DataFrame =
    spark.read.json("hdfs:///labs/laba03/weblogs.json")

  def readDomainCats(spark: SparkSession)(config: PGCfg): DataFrame =
    PGOps.readFromPostgres(spark)(config)

}
