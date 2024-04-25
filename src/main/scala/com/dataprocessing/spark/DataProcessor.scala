package com.dataprocessing.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class DataProcessor {
  def processData(): Unit = {
    val spark = SparkSession.builder()
      .appName("FDA Recall Enforcement Data Processor")
      .master("local[*]")
      .getOrCreate()

    val apiClient = new ApiClient()
    val response = apiClient.fetchData()

    import spark.implicits._

    val jsonData = Seq(response).toDS()
    val df = spark.read.json(jsonData)

    processData(df)

    spark.stop()
  }

  def processData(df: DataFrame): Unit = {
    import df.sparkSession.implicits._

    val explodedDF = df.select(explode(col("results")).as("result"))
    val processedDF = explodedDF.select("result.state", "result.classification", "result.report_date")

    val classIIIDF = processedDF.filter(col("classification") === "Class III")

    val classIIICountByState = classIIIDF.groupBy("state").count().orderBy(desc("count")).limit(10)
    classIIICountByState.show(truncate = false)

    val year2016DF = processedDF.filter(year($"report_date") === 2016)
    val avgReports2016 = year2016DF.groupBy(month($"report_date").alias("month")).count().agg(avg("count").alias("average_reports_per_month_in_2016"))
    avgReports2016.show()

    val year2017DF = processedDF.filter(year($"report_date") === 2017)
    val countByState2017 = year2017DF.groupBy("state").count().orderBy(desc("count")).limit(10)
    countByState2017.show(truncate = false)

    val yearReportCountDF = processedDF.groupBy(year($"report_date").alias("year")).count()

    val mostReportsYearRowOption = yearReportCountDF.orderBy(desc("count")).select("year", "count").collect().headOption
    val mostReportsYearOption = mostReportsYearRowOption.map(_.getInt(0))
    val mostReportsYear = mostReportsYearOption.getOrElse("N/A")
    val mostReportsCountOption = mostReportsYearRowOption.map(_.getLong(1))
    val mostReportsCount = mostReportsCountOption.getOrElse("N/A")

    val fewestReportsYearRowOption = yearReportCountDF.orderBy(asc("count")).select("year", "count").collect().headOption
    val fewestReportsYearOption = fewestReportsYearRowOption.map(_.getInt(0))
    val fewestReportsYear = fewestReportsYearOption.getOrElse("N/A")
    val fewestReportsCountOption = fewestReportsYearRowOption.map(_.getLong(1))
    val fewestReportsCount = fewestReportsCountOption.getOrElse("N/A")

    println(s"Highest year is $mostReportsYear with $mostReportsCount reports")
    println(s"Lowest year is $fewestReportsYear with $fewestReportsCount reports")
  }
}