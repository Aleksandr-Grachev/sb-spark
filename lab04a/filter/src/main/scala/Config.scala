import org.apache.spark.SparkConf
object Config {
  import AppParams._

  case class AppParams(sparkConf: {def getOption(k:String):Option[String]} 
  )(log: SimpleLog) {

    val KAFKA_BOOTSTRAP ="spark-master-1:6667"

    val topicName = sparkConf.getOption("spark.filter.topic_name").getOrElse {
     throw new Exception("We don't have any topic_name in the Spark conf")
    }

    val offset =
      sparkConf.getOption("spark.filter.offset")

    val outPrefix = sparkConf.getOption("spark.filter.output_dir_prefix")

    def outputBase =
      outPrefix
        .collect {
          case p: String if p.startsWith("/") => p
          case o: String                      => s"$DEFAULT_OUT/$o"
        }
        .getOrElse {
          log.show(s"Using default out path[${DEFAULT_OUT}]")
          DEFAULT_OUT
        }

    // Преобразуем offset в правильный JSON формат
    def startingOffsets =
      offset match {
        case Some("earliest") | None =>
          "earliest"
        case Some(o)                 =>
          s"""{"$topicName":{"0":$offset}}"""
      }

  }

  object AppParams {

    val DEFAULT_OUT = s"/user/aleksandr.grachev"

    trait SimpleLog {
      def show(s: String):Unit
    }

    // implicit class SparkConfOps(sparkConf: SparkConf) {
    //   def getKeyValue(key: String): Option[String] =
    //     if (sparkConf.contains(key)) {
    //           Option(sparkConf.get(key))
    //     } else {
    //       None
    //     }
    // }

  }

}
