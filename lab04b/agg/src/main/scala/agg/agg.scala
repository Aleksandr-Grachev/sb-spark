import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object agg {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("alexander_grachev_lab04b")
      .getOrCreate()

    import spark.implicits._

    val KAFKA_INPUT_TOPIC  = "aleksandr_grachev"
    val KAFKA_OUTPUT_TOPIC = "aleksandr_grachev_lab04b_out"
    val KAFKA_TRIGGER_TIME = "5 seconds"
    val KAFKA_MASTER       = "spark-master-1:6667"

    // UTC, or dates could be wrong
    spark.conf.set("spark.sql.session.timeZone", "UTC")// TODO: ?

    // Kafka input topic
    val inputDFTopic =
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_MASTER)
        .option("subscribe", KAFKA_INPUT_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .select(col("value").cast(StringType))

    // JSON схема
    val schema =
      new StructType()
        .add("timestamp", LongType)
        .add("uid", StringType)
        .add("event_type", StringType)
        .add("item_price", IntegerType)

    val parsed =
      inputDFTopic
        .select(
          // парсим в StructType
          from_json(col("value"), schema).as("data")
        )                 // TODO: filter out bad json ?
        .select("data.*") // разворачиваем StructTyle
        .withColumn(
          "event_ts",
          (col("timestamp") / 1000) // превращаем в секунды
            .cast(LongType)
        )
        .withColumn(
          "event_time",
          to_timestamp(
            col("event_ts")
          ) // в том числе принимает unix epoch _secodns_
        )

    // Aggregation
    val computed =
      parsed
        .withWatermark("event_time", "2 hours")
        // ТЗ: подсчитать метрики за каждый час
        .groupBy(window(col("event_time"), "1 hour"))
        .agg(
          // общая сумма продаж
          sum(when(col("event_type") === "buy", col("item_price")).otherwise(0))
            .as("revenue"),
          // число посетителей
          count(when(col("uid").isNotNull, 1)).as("visitors"),
          // число покупок
          count(when(col("event_type") === "buy", 1)).as("purchases")
        )
        .withColumn( // средний чек
          "aov",
          when(col("purchases") === 0, lit(0.0))
            .otherwise(col("revenue") / col("purchases"))
        )
        .select(
          unix_timestamp(col("window.start")).as("start_ts"),
          unix_timestamp(col("window.end")).as("end_ts"),
          col("revenue"),
          col("visitors"),
          col("purchases"),
          col("aov")
        )
        .select(
          to_json(
            struct(
              col("start_ts"),
              col("end_ts"),
              col("revenue"),
              col("visitors"),
              col("purchases"),
              col("aov")
            )
          ).alias("value")
        )

    // Kafka output topic

    def writeOutToKafka(df: DataFrame): StreamingQuery =
      df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_MASTER)
        .option("topic", KAFKA_OUTPUT_TOPIC)
        // .option("checkpointLocation", "./chk_lab04b")
        .outputMode("update")
        .trigger(
          // тригер микробатча всегда на sink
          org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds")
        )
        .start()

    def writeToConsole(
        df: DataFrame
    ): StreamingQuery =
      df.writeStream
        .format("console")                                   // Формат вывода: консоль
        .outputMode("update")
        .option("truncate", "false")                         // Не обрезать длинные строки
        .trigger(Trigger.ProcessingTime(KAFKA_TRIGGER_TIME)) // Частота триггера
        .start() // Запуск потока

    def writeToCell(df: DataFrame): StreamingQuery =
      df.writeStream
        .foreachBatch { (batchDF:DataFrame, batchId: Long) =>
          println(s"\n=== Batch $batchId (${new java.util.Date()}) ===")
          batchDF
            .limit(20)
            .show(truncate = false)
        }
        .trigger(Trigger.ProcessingTime(KAFKA_TRIGGER_TIME)) // Частота триггера
        .start()

    val query = writeOutToKafka(computed)

    query.awaitTermination()
  }
}
