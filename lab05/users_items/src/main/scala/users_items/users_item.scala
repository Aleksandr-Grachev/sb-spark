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

    /** Пример строки входных данных из lab04a:
      * ```
      * {"event_type":"view",
      * "category":"Mobile-phones", "item_id":"Mobile-phones-5",
      * "item_price":"1029", "timestamp":1588107480000,
      * "uid":"1f295269-22ce-418a-8245-269646990dce", "date":"20200428"
      * }
      * ```
      */
    // читаем входные данные view
    val viewDf = readVisits(spark, s"$inputDir/view")
      .withColumn(
        "item_col",
        concat(lit("view_"), normalizeItem(col("item_id")))
      ) // добавляем нормализованную колонку со значением view_mobile_phones_5 (для примера)

    // читаем входные данные buy)
    val buyDf = readVisits(spark, s"$inputDir/buy")
      .withColumn(
        "item_col",
        concat(lit("buy_"), normalizeItem(col("item_id")))
      )

    // объединяем по колонкам
    val eventsDf = viewDf.unionByName(buyDf)

    // users x items через pivot
    val newMatrix = eventsDf
      .groupBy(col("uid"))
      .pivot(col("item_col"))
      .agg(count(lit(1)))
      .na
      .fill(0)

    // считаем максимальную дату в данных (UTC)
    // TЗ: самые последние данные датированные данные
    val maxDate =
      eventsDf
        .select(max(col("event_time")).as("ts"))
        .as[Long]
        .collect()
        .headOption
        .getOrElse {
          throw new Exception("There isn't any event time in united events")
        }

    val dateStr = formatDate(maxDate)

    val finalMatrix =
      if (update == "1" && exists(outputDir)) {
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

  /** Названия полей item_id должны быть нормализованы
    *   - приведены к нижнему регистру,
    *   - пробелы и тире заменены на подчерк.
    *   - т.е. например `view_computers_2" и так далее
    *
    * @param colItem
    * @return
    */
  def normalizeItem(colItem: Column): Column =
    lower(
      regexp_replace(
        regexp_replace(colItem, "[\\s-]+", "_"),
        "[^a-zA-Z0-9_]",
        ""
      )
    )

  /** В выходной директории вы должны записать матрицу `users-items` в формате
    * parquet по пути (в подпапке) `YYYYMMDD`
    *
    * @param ts
    * @return
    */
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

  // читает последний датафрейм по пути outputDir
  // сортируюя лексикографически по имени
  def readLatestMatrix(spark: SparkSession, outputDir: String): DataFrame = {
    val fs   = org.apache.hadoop.fs.FileSystem
      .get(spark.sparkContext.hadoopConfiguration)
    val base = new org.apache.hadoop.fs.Path(outputDir)

    val latestPath = fs
      .listStatus(base)
      .filter(_.isDirectory)
      .map(_.getPath.getName)
      .sorted
      .lastOption
      .getOrElse {
        throw new Exception(
          s"There isn't latest path in base[${base.toString()}]"
        )
      }

    spark.read.parquet(s"$outputDir/$latestPath")
  }

  // мержит матрицы из  uid(by_xx, view_xx)
  def mergeMatrices(oldDf: DataFrame, newDf: DataFrame): DataFrame = {

    val allCols: Array[String] =
      (oldDf.columns ++ newDf.columns).distinct

    val oldAligned = addMissingCols(oldDf, allCols)
    val newAligned = addMissingCols(newDf, allCols)

    val aggExp = // суммирование всех колонок кроме uid
      allCols
        .filter(_ != "uid")
        .map(colName => sum(col(colName)).as(colName))

    oldAligned
      .unionByName(newAligned)
      .groupBy("uid")
      .agg(aggExp.head, aggExp.tail: _*)
  }

  //TЗ можно добавить недостающие колонки в датасет и заполнить их нулями.
  def addMissingCols(df: DataFrame, cols: Seq[String]): DataFrame =
    cols.foldLeft(df) { (acc, c) =>
      if (acc.columns.contains(c)) acc else acc.withColumn(c, lit(0))
    }
    
}
