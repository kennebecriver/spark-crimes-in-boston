package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object BostonCrimesMap {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("HelloSpark")
      .master("local[*]")
      .getOrCreate()

    val crimeFacts = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0)) // src/data/crime.csv

    spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(1)) // src/data/offense_codes.csv
      .createOrReplaceTempView("offenseCodes")

    val offenseCodes = spark.sql("""
          SELECT CODE, trim(regexp_replace(lower(NAME), ' -.*', '')) as crime_type
          FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY CODE order by NAME) as rn FROM offenseCodes) _
          WHERE rn = 1
          """)

    val offenseCodesBroadcast = broadcast(offenseCodes)

    crimeFacts
      .join(offenseCodesBroadcast, crimeFacts("OFFENSE_CODE") === offenseCodesBroadcast("CODE"))
      .select("DISTRICT","YEAR", "MONTH", "Lat", "Long", "crime_type")
      .createOrReplaceTempView("robberyStatsTable")

    val t0 = spark.sql("""
      SELECT DISTRICT, concat_ws(', ', collect_list(crime_type)) as crime_type FROM
        (
          SELECT *, ROW_NUMBER() OVER(PARTITION BY DISTRICT order by crime_type_total DESC) as rn
          FROM (SELECT DISTRICT, crime_type, count(1) as crime_type_total FROM robberyStatsTable group by DISTRICT, crime_type) _
        ) __
        WHERE rn <= 3 group by DISTRICT
      """)

    val t1 = spark.sql("""
      SELECT
      DISTRICT,
      SUM(crimes_total) as crimes_total,
      percentile_approx(crimes_total, 0.5) as crimes_monthly
      FROM
      (
        SELECT
        DISTRICT,
        concat(YEAR, MONTH) as YEAR_MONTH,
        count(1) as crimes_total
        FROM
        robberyStatsTable group by DISTRICT, YEAR_MONTH
        order by YEAR_MONTH
      ) _
      GROUP BY DISTRICT
      """)

    val t2 = spark.sql("""
      SELECT
      DISTRICT,
      avg(Lat) as lat,
      avg(Long) as lng
      FROM
      robberyStatsTable
      GROUP BY DISTRICT
      """)

    val result = t0.join(t1,Seq("DISTRICT")).join(t2,Seq("DISTRICT"))
    result.show(false)
    println(s"...SAVING: please stand by...")
    result.coalesce(1).write.format("parquet").mode("append").save(args(2) + "/result.parquet")
    println(s"Result has been saved to ${args(2)}")
  }
}
