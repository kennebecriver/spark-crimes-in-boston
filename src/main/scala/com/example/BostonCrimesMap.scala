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

    import spark.implicits._

    val crimeFacts = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/data/crime.csv")

    val offenseCodes = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/data/offense_codes.csv")

    val offenseCodesBroadcast = broadcast(offenseCodes)

    crimeFacts
      .join(offenseCodesBroadcast, $"CODE" === $"OFFENSE_CODE")
      .select("DISTRICT")
      .createOrReplaceTempView("robberyStatsTable")

    val result = spark.sql("" +
      "SELECT DISTRICT from robberyStatsTable limit 15" +
      "")

    result.show
  }
}
