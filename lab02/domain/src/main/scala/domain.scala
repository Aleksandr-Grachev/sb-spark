import org.apache.spark.sql.SparkSession
import scala.util.{Failure, Success, Try}
import java.net.URI
import java.io.PrintWriter

import scala.util.control.NonFatal
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import java.net.URLDecoder

object domain {

  import utils._

  /** Коеффициент Отиаи:
    *   - ∣A∩B∣ — количество элементов, которые одновременно принадлежат и
    *     множеству A, и множеству B.
    *   - ∣A∣— общее количество элементов в множестве A.
    *   - ∣B∣ — общее количество элементов в множестве B.
    */

  def main(args: Array[String]): Unit = {
    val autousersPath = "hdfs:///labs/laba02/autousers.json"
    val logsPath      = "hdfs:///labs/laba02/logs"
    // Внутри метода
    val spark         = SparkSession
      .builder()
      .appName("aleksandr_grachev_lab02")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val autoIds = sc
      .textFile(autousersPath)
      .flatMap { rawAuto =>
        implicit val formats: DefaultFormats.type = DefaultFormats
        val json                                  = parse(rawAuto)
        (json \ "autousers").extract[List[String]].toSet
      }
      .collect

    val bcAuto = sc.broadcast(autoIds)
    // Some auto examples
    autoIds.take(10).foreach(println)

    // loading logs
    val rawLogs = sc.textFile(logsPath)

    // ид пользователя -> домен посещения вырезанный из url
    val uidAndDomain = rawLogs.flatMap { line =>
      val parts = line.split("\t", 3).map(_.trim)

      parts.toList match {
        case uid :: _ :: rawUrl :: _ if uid.isNonEmpty && rawUrl.isNonEmpty =>
          getDomain(rawUrl).map { domain =>
            uid -> domain
          }

        case _ => None
      }
    }

    println(s"Here is ui and domain map[${uidAndDomain.count()}]")
    println(uidAndDomain.take(10).foreach(println))

    // домен -> (признак визита автомобилиста, общий признак визита)
    val domainPairs: RDD[(String, (Long, Long))] = uidAndDomain.flatMap {
      case (uid, domain) =>
        Try {
          val autoVisit: Long = if (bcAuto.value.contains(uid)) 1L else 0L
          Some((domain, (autoVisit, 1L)))
        } match {
          case Success(Some(v)) => Some(v)
          case _                => None
        }
    }

    println(s"Here is domain pairs")
    println(domainPairs.take(10).foreach(println))

    // RDD domen -> (число визитов автомобилистов, общее число визитов)
    val domainAgg =
      domainPairs
        .reduceByKey { case ((avLeft, totalLeft), (avRight, totalRight)) =>
          (avLeft + avRight, totalLeft + totalRight)
        }

    println(s"Here is domain agg map[${domainAgg.count()}]")
    println(domainAgg.take(10).foreach(println))

    // все визиты автомобилистов по всем доменам.
    val autoUsersVisitsTotal =
      domainAgg
        .map { case (_, (aVisitTotal, _)) =>
          aVisitTotal
        }
        .sum()
        .toLong

    println(s"Here is auto users visit total:$autoUsersVisitsTotal")
    //
    val relevanceRDD = domainAgg.flatMap {
      // totalAutoForDomain количество визитов автомобилистов на домен |A ∩ B|
      // totalVisitsForDomain общее количество визитов на домен (∣A∣).
      // autoUsersVisitsTotal общее количество визитов автомобилистов (∣B∣).
      case (domain, (totalAutoForDomain, totalVisitsForDomain)) =>
        if (totalVisitsForDomain <= 0L) None
        else {
          val num = totalAutoForDomain.toDouble * totalAutoForDomain.toDouble
          val den = totalVisitsForDomain.toDouble * autoUsersVisitsTotal
          if (den == 0.0) None
          else Some((domain, num / den))
        }
    }

    println(s"Here is relevance :${relevanceRDD.count()}")

    // domain -> relevance sorted
    val sorted: RDD[(String, Double)] = relevanceRDD
      .map { case (domain, rel) => ((-rel, domain), (domain, rel)) }
      .sortByKey()
      .map { case (_, domaInAndRelevancePair: (String, Double)) =>
        domaInAndRelevancePair
      }

    val top200 = sorted.take(200)

    println(s"Here is top200 :${top200.size}")

    val outLines = top200.map { case (domain, rel) =>
      val relStr = f"$rel%.15f"
      s"$domain\t$relStr"
    }.toSeq

    val home     = System.getProperty("user.home")
    val localOut = s"$home/laba02_domains.txt"

    val pw = new PrintWriter(localOut)

    try
      outLines.foreach(pw.println)
    finally
      pw.close()

    println(s"Saved final result to LOCAL FS: $localOut")

    spark.stop()
  }
}

object utils {

  implicit class StrOps(str: String) {
    def getNonEmpty: Option[String] =
      Option(str).collect {
        case s: String if !s.isEmpty && !s.equalsIgnoreCase("-") =>
          s
      }

    def isNonEmpty: Boolean = getNonEmpty.nonEmpty
  }

  def getDomain(rawUrl: String): Option[String] = {
    val parsed =
      for {
        decoded <- Try(URLDecoder.decode(rawUrl, "UTF-8")).toEither

        uri <- Try(URI.create(decoded)).toEither

        scheme <- Either.cond(
                    Option(uri.getScheme()).exists(s =>
                      s.equalsIgnoreCase("http") || s.equalsIgnoreCase("https")
                    ),
                    uri.getScheme(),
                    new IllegalArgumentException(
                      s"No http/https scheme in uri [$rawUrl]"
                    )
                  )

        host <- Either.cond(
                  Option(uri.getHost()).exists(_.isNonEmpty),
                  uri.getHost().trim.toLowerCase,
                  new IllegalArgumentException(
                    s"No host in uri [$rawUrl]"
                  )
                )

        domain = if (host.startsWith("www.")) host.drop(4) else host

      } yield domain

    parsed match {
      case Left(e)  =>
        println(s"Error parsing [$rawUrl]: ${e.getMessage}")
        None
      case Right(d) =>
        Some(d)
    }
  }

}
