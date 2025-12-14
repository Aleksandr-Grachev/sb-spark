import org.apache.spark.sql.SparkSession
import scala.util.{Failure, Success, Try}
import java.net.URI

import scala.util.control.NonFatal
import org.apache.spark.rdd.RDD
import java.net._

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.net.{URL, URLDecoder}
import scala.util.Try

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.net.{URL, URLDecoder}
import scala.util.Try

// Проект должен компилироваться и запускаться следующим образом (важны все строки):

// ```
// export SPARK_DIST_CLASSPATH=$(hadoop classpath)
// export PYSPARK_PYTHON=/opt/miniconda/envs/2024/bin/python
// export PYSPARK_DRIVER_PYTHON=/opt/miniconda/envs/2024/bin/python
//
// cd lab03/data_mart
// sbt package
// /opt/spark-3.4.3/bin/spark-submit
//  --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.14.2, \
//             com.datastax.spark:spark-cassandra-connector_2.12:3.5.1, \
//             org.postgresql:postgresql:42.7.3, \
//             joda-time:joda-time:2.12.7 \
// --class data_mart target/scala-2.12/data_mart_2.12-1.0.jar

object data_mart {

  import Utils._

  def main(args: Array[String]): Unit = {

    val config = Config.newprolab
    // Внутри метода
    val spark  = SparkSession
      .builder()
      .appName("aleksandr_grachev_lab03")
      .config("spark.cassandra.connection.host", config.cassandra.host)
      .config("spark.cassandra.connection.port", config.cassandra.port)
      .getOrCreate()

    import spark.implicits._

    // ----raw data------
    val clients = RawData.readClients(spark)
    println("Clients")
    clients.show()
    println("Cassandra total rows")
    clients.count() // should be 36138
    // --
    val shop       = RawData.readShop(spark)(config.es)
    println("Shop")
    shop.show()
    // --
    val webLogs    = RawData.readWeblogs(spark)
    println("Web Logs")
    webLogs.show()
    // --
    val domainCats = RawData.readDomainCats(spark)(config.pg)
    println("Domain Cats")
    domainCats.show()

    // ----transform-----

    val clientsWithAgeCategory = Transform.clientsWithAgeCategory(clients)
    // проверка из ТЗ
    val byGenderAndAgeCat      =
      clientsWithAgeCategory
        .groupBy(col("gender"), col("age_cat"))
        .count()
        .orderBy(col("gender"), col("age_cat"))

    byGenderAndAgeCat.show(100)

    // --
    val (shopAgg, shopCategories) = Transform.pivotShop(shop)
    println("Shop pivot")
    shopAgg.show()

    println(s"Shop Categories[${shopCategories.mkString(",")}]")

    // --
    val (webAgg, normalizedDomainCats) =
      Transform.pivotWeb(webLogs, domainCats)
    println("HDFS Web logs pivot")
    webAgg.show()
    println(s"Domain Categories[${normalizedDomainCats.mkString(",")}]")

    // --- calculate result
    val result = clientsWithAgeCategory
      .join(shopAgg, Seq("uid"), "left")
      .join(webAgg, Seq("uid"), "left")
      .na
      .fill(0)
    println("Calculated result")
    result.show()

    // ------ create table
    PGOps.createClientsTable(
      shopCategories = shopCategories,
      webCategories = normalizedDomainCats
    )(config.pg)

    // -------write the result------------
    println(s"Start write the result")
    PGOps.writeToPostgres(result)(config.pg)
    println(s"write result end")
    // give checker access
    PGOps.grantTable(config.pg)
    println(s"table select granted")
    spark.stop()
  }

}
