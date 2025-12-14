import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import org.apache.spark.sql._

object users_items {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("users_items")
      .getOrCreate()

    import spark.implicits._

    val inputDir  = spark.conf.get("spark.users_items.input_dir")
    val outputDir = spark.conf.get("spark.users_items.output_dir")
    val update    = spark.conf.get("spark.users_items.update", "1")

    // читаем входные данные (view + buy)
    val viewDf = readVisits(spark, s"$inputDir/view")
      .withColumn(
        "item_col",
        concat(lit("view_"), normalizeItem(col("item_id")))
      )

    val buyDf = readVisits(spark, s"$inputDir/buy")
      .withColumn(
        "item_col",
        concat(lit("buy_"), normalizeItem(col("item_id")))
      )

    val eventsDf = viewDf.unionByName(buyDf)

    // считаем максимальную дату в данных (UTC)
    val maxDate = eventsDf
      .select(max(col("event_time")).as("ts"))
      .as[Long]
      .collect()
      .head

    val dateStr = formatDate(maxDate)

    // users x items через pivot
    val newMatrix = eventsDf
      .groupBy(col("uid"))
      .pivot(col("item_col"))
      .agg(count(lit(1)))
      .na
      .fill(0)

    val finalMatrix = if (update == "1" && exists(outputDir)) {
      val prev = readLatestMatrix(spark, outputDir)
      mergeMatrices(prev, newMatrix)
    } else {
      newMatrix
    }

    finalMatrix.write
      .mode("overwrite")
      .parquet(s"$outputDir/$dateStr")

    spark.stop()
  }

  // ---------- helpers ----------

  def readVisits(spark: SparkSession, path: String): DataFrame =
    spark.read
      .parquet(path)
      .select(
        col("uid"),
        col("item_id"),
        col("event_time").cast(LongType)
      )

  def normalizeItem(colItem: Column): Column =
    lower(
      regexp_replace(
        regexp_replace(colItem, "[\\s-]+", "_"),
        "[^a-zA-Z0-9_]",
        ""
      )
    )

  def formatDate(ts: Long): String =
    DateTimeFormatter
      .ofPattern("yyyyMMdd")
      .withZone(ZoneOffset.UTC)
      .format(Instant.ofEpochMilli(ts))

  def exists(path: String): Boolean = {
    val fs = org.apache.hadoop.fs.FileSystem.get(
      new org.apache.hadoop.conf.Configuration()
    )
    fs.exists(new org.apache.hadoop.fs.Path(path))
  }

  def readLatestMatrix(spark: SparkSession, outputDir: String): DataFrame = {
    val fs   = org.apache.hadoop.fs.FileSystem
      .get(spark.sparkContext.hadoopConfiguration)
    val base = new org.apache.hadoop.fs.Path(outputDir)

    val latestPath = fs
      .listStatus(base)
      .filter(_.isDirectory)
      .map(_.getPath.getName)
      .sorted
      .last

    spark.read.parquet(s"$outputDir/$latestPath")
  }

  def mergeMatrices(oldDf: DataFrame, newDf: DataFrame): DataFrame = {
    val allCols = (oldDf.columns ++ newDf.columns).distinct

    val oldAligned = addMissingCols(oldDf, allCols)
    val newAligned = addMissingCols(newDf, allCols)

    val aggExp = allCols.filter(_ != "uid").map(c => sum(col(c)).as(c))
    oldAligned
      .unionByName(newAligned)
      .groupBy("uid")
      .agg(aggExp.head, aggExp.tail: _*)
  }

  def addMissingCols(df: DataFrame, cols: Seq[String]): DataFrame =
    cols.foldLeft(df) { (acc, c) =>
      if (acc.columns.contains(c)) acc else acc.withColumn(c, lit(0))
    }
}
