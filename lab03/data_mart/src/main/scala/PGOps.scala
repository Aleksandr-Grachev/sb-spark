import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object PGOps {

  def readFromPostgres(spark: SparkSession)(config: PGCfg): DataFrame =
    spark.read
      .format("jdbc")
      .option(
        "url",
        s"jdbc:postgresql://${config.host}:${config.port}/${config.in.db}"
      )
      .option("dbtable", "domain_cats")
      .option("user", config.user)
      .option("password", config.password)
      .option("driver", "org.postgresql.Driver")
      .load()

  def writeToPostgres(df: DataFrame)(config: PGCfg): Unit =
    df.write
      .format("jdbc")
      .option(
        "url",
        s"jdbc:postgresql://${config.host}:${config.port}/${config.out.db}"
      )
      .option("dbtable", "clients")
      .option("user", config.user)
      .option("password", config.password)
      .option("driver", "org.postgresql.Driver")
      .option("truncate", "true")
      .mode("overwrite")
      .save()

  def createClientsTable(
      shopCategories: Seq[String],
      webCategories: Seq[String]
  )(config: PGCfg): Unit = {

    val conn = getConnection(config)
    val stmt = conn.createStatement()
    // first we drop table
    stmt.executeUpdate("DROP TABLE IF EXISTS clients CASCADE")

    // next we create one
    val cols =
      Seq("uid text PRIMARY KEY", "gender text", "age_cat text") ++
        shopCategories
          .map(cat => s"$cat bigint") ++
        webCategories.map(cat => s"$cat bigint")

    // create the
    val sql =
      s"""
       |CREATE TABLE clients (
       |${cols.mkString(",")}
       |);
       |""".stripMargin

     println(s"TABLE SQL[$sql]")  

    stmt.executeUpdate(sql)

    stmt.close()
    conn.close()

    println("Table clients created")
  }

  def grantTable(config: PGCfg): Unit = {
    val conn = getConnection(config)
    val stmt = conn.createStatement()
    stmt.execute("GRANT SELECT ON clients TO labchecker2")
    conn.close()
  }

  def getConnection(config: PGCfg) = {
    Class.forName("org.postgresql.Driver")
    val url =
      s"jdbc:postgresql://${config.host}:${config.port}/${config.out.db}"

    java.sql.DriverManager.getConnection(url, config.user, config.password)
  }

}
