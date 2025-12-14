import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

object Transform {

  // client
  def clientsWithAgeCategory(df: DataFrame): DataFrame =
    df.withColumn(
      "age_cat",
      when(col("age").between(18, 24), "18-24")
        .when(col("age").between(25, 34), "25-34")
        .when(col("age").between(35, 44), "35-44")
        .when(col("age").between(45, 54), "45-54")
        .otherwise(">=55")
    ).select(col("uid"), col("gender"), col("age_cat"))

  // shop
  // преобразование данных их лога магазина
  def pivotShop(elasticShopData: DataFrame): (DataFrame, Array[String]) = {

    // фильтруем набор
    val withFilteredUID = elasticShopData
      .filter(col("uid").isNotNull)
      .select("uid", "category")

    // все категории из датафремйма
    val shopCategories: Array[String] =
      withFilteredUID
        .select("category")
        .distinct()
        .collect()
        .map(r => r.getString(0))
        .sorted

    val normalizedShopCategories = // нормализуем имена по ТЗ
      shopCategories
        .map(Utils.normalizeShopCategory)
        .sorted

    val outColNames = Seq("uid") ++ normalizedShopCategories

    val ret = withFilteredUID
      .groupBy("uid")
      // Гарантированно создаёт все указанные колонки
      // даже если в данных нет одной из категорий
      // Создаёт эти колонки ровно в указанном порядке, что критично для чекера
      .pivot(
        "category",
        shopCategories
      )        // превращатем значение котонки в имена столбцов
      .count() // с подсчетом
      .na      // удаляет строки в которых сплошные Null
      .fill(0) // заменяет Null & NaN нулем в числовых колонках
      .toDF(outColNames: _*)

    ret -> normalizedShopCategories
  }

  def pivotWeb(
      hdfsLogsData: DataFrame,
      pgDomainCatsData: DataFrame
  ): (DataFrame, Array[String]) = {

    // explode URLs
    val flat = hdfsLogsData
      .filter(col("uid").isNotNull)
      .withColumn("visit", explode(col("visits")))
      .select(
        col("uid"),
        col("visit.url").as("url")
      )

    val logsWithDomain = flat
      .withColumn(
        "domain",
        regexp_replace(Utils.decodeUrlAndGetDomain(col("url")), "^www\\.", "")
      )
      .filter(col("domain") =!= "")

    val withCats = logsWithDomain
      .join(pgDomainCatsData, Seq("domain"), "left")
      .filter(col("category").isNotNull)

    val domainCaterories: Array[String] =
      pgDomainCatsData
        .select("category")
        .distinct()
        .collect()
        .map(r => r.getString(0))
        .sorted

    val normalizedDomainCats =
      domainCaterories
        .map(Utils.normalizeWebCategory)
        .sorted

    val outCols = Seq("uid") ++ normalizedDomainCats

    val ret = withCats
      .groupBy("uid")
      .pivot("category", domainCaterories)
      .count()
      .na
      .fill(0)
      .toDF(outCols: _*)

    ret -> normalizedDomainCats
  }

}
