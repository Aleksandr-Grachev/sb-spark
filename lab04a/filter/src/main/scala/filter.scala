import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import Config.AppParams

object filter {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("aleksandr_grachev_lab04a")
      .getOrCreate()

    // UTC, or dates could be wrong
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val logger = new Config.AppParams.SimpleLog {
      override def show(s: String) = println(s)
    }


    val appParams = Config.AppParams(spark.sparkContext.getConf)(logger)

    // Схема входящего JSON
    val schema = new StructType()
      .add("event_type", StringType)
      .add("category", StringType)
      .add("item_id", StringType)
      .add("item_price", LongType)
      .add("uid", StringType)
      .add("timestamp", LongType)

    // Читаем Kafka 
    val dfRaw = spark.read
      .format("kafka")
      .option(
        "kafka.bootstrap.servers",
        appParams.KAFKA_BOOTSTRAP
      )
      .option("session.timeout.ms", 30000)
      .option("subscribe", appParams.topicName)
      .option("startingOffsets", appParams.startingOffsets)
      .load()

    // value — bytes → строка → JSON
    val df = dfRaw
      .select(col("value").cast("string").alias("json"))
      .select(from_json(col("json"), schema).alias("data"))
      .select("data.*")
      .withColumn(
        "date",
        date_format((col("timestamp") / 1000).cast("timestamp"), "yyyyMMdd")
      )
      .withColumn("p_date", col("date")) // для partitionBy

    // filters
    val dfView = df.filter(col("event_type") === "view")
    val dfBuy  = df.filter(col("event_type") === "buy")

    // paths
    val pathView = s"${appParams.outputBase}/view"
    val pathBuy  = s"${appParams.outputBase}/buy"

    // удаляем выходные каталоги перед записью, ТЗ
    import org.apache.hadoop.fs.{FileSystem, Path}
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    Seq(pathView, pathBuy).foreach { p =>
      val h = new Path(p)
      if (fs.exists(h)) fs.delete(h, true)
    }

    // Пишем JSON (одна строка = одно событие)
    dfView.write
      .mode("overwrite")
      .partitionBy("p_date")
      .json(pathView)

    dfBuy.write
      .mode("overwrite")
      .partitionBy("p_date")
      .json(pathBuy)

    spark.stop()
  }
}
