import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object agg {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("alexander_grachev_lab04b")
      .getOrCreate()

    import spark.implicits._
    // UTC, or dates could be wrong
    //spark.conf.set("spark.sql.session.timeZone", "UTC") TODO: ?

    val KAFKA_INPUT_TOPIC  = "alexander_grachev"
    val KAFKA_OUTPUT_TOPIC = "alexander_grachev_lab04b_out"

    // Kafka input topic
    val inputDFTopic =
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "spark-master-1:6667")
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
        .groupBy(window(col("event_time"), "1 hour"))
        .agg(
          sum(when(col("event_type") === "buy", col("item_price")).otherwise(0))
            .as("revenue"),
          count(when(col("uid").isNotNull, 1)).as("visitors"),
          count(when(col("event_type") === "buy", 1)).as("purchases")
        )
        .withColumn(
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
    val query = computed.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "master01:9092")
      .option("topic", KAFKA_OUTPUT_TOPIC)
      // .option("checkpointLocation", "./chk_lab04b")
      .outputMode("update")
      .trigger(
        org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds")
      )
      .start()

    query.awaitTermination()
  }
}
