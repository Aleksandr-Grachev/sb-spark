import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.util.Try
import java.net.URL
import java.net.URLDecoder

object Utils {

  implicit class StrOps(str: String) {
    def getNonEmpty: Option[String] =
      Option(str).collect {
        case s: String if !s.isEmpty && !s.equalsIgnoreCase("-") =>
          s
      }

    def isNonEmpty: Boolean = getNonEmpty.nonEmpty
  }

  def normalizeShopCategory(c: String): String =
    "shop_" + c.toLowerCase.replace(" ", "_").replace("-", "_")

  def normalizeWebCategory(c: String): String =
    "web_" + c.toLowerCase.replace(" ", "_").replace("-", "_")

  // Пример декодирования URL и извлечения домена с помощью UDF:
  def decodeUrlAndGetDomain: UserDefinedFunction =
    udf { (url: String) =>
      Try {
        new URL(URLDecoder.decode(url, "UTF-8")).getHost
      }.getOrElse("")
    }
  // transformedLogs.select(col("uid"), decodeUrlAndGetDomain(col("url")).alias("domain"))

}
