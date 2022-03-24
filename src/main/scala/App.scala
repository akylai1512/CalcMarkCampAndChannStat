package org.example


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Options.HandleOpt.path
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.MapFile.Writer.compression
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, desc, row_number, to_date, to_timestamp}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.plans.table
import org.example.CalcBySQL.{topCampBySQL, topChanBySQL, topChanBySQLbyPeriod, topCompBySQLbyPeriod}
import org.example.calcBySparkApi.{popChanalSparkApi, topCampBySparkApi}

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.util.Locale


class App {
}
object  App  {

  def dataTypes: DateTimeFormatter =
    new DateTimeFormatterBuilder()
      .appendOptional(DateTimeFormatter.ofPattern("dd/MM/yy"))
      .appendOptional(DateTimeFormatter.ofPattern("dd.MM.yyyy"))
      .appendOptional(DateTimeFormatter.ofPattern("dd-MM-yyyy"))
      .appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
      .appendOptional(DateTimeFormatter.ofPattern("ddMMMuu")).toFormatter(Locale.ENGLISH)

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder
      .appName("Apache Spark")
      .master("local")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")

   val parquetTarget = spark.read.option("mergeSchema", "true").parquet("src/resources/capstone-dataset/parquet_dataset/targetTable.parquet")
    //parquetTarget.show()

    val csvTarget =spark.read.format("csv").options(Map("inferSchema" -> "true", "sep" -> ",", "header" -> "true")).csv("src/resources/capstone-dataset/csv_dataset/targetTable.csv/*")
  //  csvTarget.show()

    /**
     *  Top Campaigns: by SparkAPI
     */
    //topCampBySparkApi(csvTarget).show()


    /**
     * Channels by SparkAPI
     */

   // popChanalSparkApi(csvTarget).show()

    /**
     *  Top Campaigns SQL version :
     *  What are the Top 10 marketing campaigns that bring the biggest revenue (based on billingCost of confirmed purchases)?
     */
   // topCampBySQL(spark ,csvTarget ).show()
   // topCampBySQL(spark ,csvTarget ).explain(extended=true)
    //spark.time(topCampBySQL(spark ,csvTarget ))


    //topCampBySQL(spark ,parquetTarget ).show()
   //topCampBySQL(spark ,parquetTarget ).explain(extended=true)
   // spark.time(topCampBySQL(spark ,parquetTarget ))


    /**
     * Channels engagement performance SQL version :
     * - What is the most popular (i.e. Top) channel that drives the highest amount of unique sessions (engagements)
     * with the App in each campaign?
     */

   //topChanBySQL(spark ,csvTarget).show()
   //topChanBySQL(spark ,csvTarget).explain(extended=true)
   //spark.time( topChanBySQL(spark ,csvTarget))


    //topChanBySQL(spark ,parquetTarget).show()
    //topChanBySQL(spark ,parquetTarget).explain(extended=true)
    //spark.time( topChanBySQL(spark ,parquetTarget))

    /**
     *  Top Campaigns:
     *  What are the Top 10 marketing campaigns that bring the biggest revenue (based on billingCost of confirmed purchases)?
     */

    val s = LocalDate.parse("2020-11-11",  dataTypes)
    val l = LocalDate.parse("11Nov20",  dataTypes)
    //topCompBySQLbyPeriod(spark, csvTarget, s).show()
    //topCompBySQLbyPeriod(spark, csvTarget, s).explain(extended=true)
    //spark.time(topCompBySQLbyPeriod(spark, csvTarget, s))


   //topCompBySQLbyPeriod(spark, parquetTarget, l).show()
    //topCompBySQLbyPeriod(spark, parquetTarget, l).explain(extended=true)
    //spark.time(topCompBySQLbyPeriod(spark, parquetTarget, l))

    /**
     * Channels engagement performance SQL version :
     * - What is the most popular (i.e. Top) channel that drives the highest amount of unique sessions (engagements)
     * with the App in each campaign?
     */


    //topChanBySQLbyPeriod(spark, csvTarget, s).show()
    //topChanBySQLbyPeriod(spark, csvTarget, s).explain(extended=true)
    //spark.time(topChanBySQLbyPeriod(spark, csvTarget, s))


    //topChanBySQLbyPeriod(spark, parquetTarget, l).show()
    //topChanBySQLbyPeriod(spark, parquetTarget, l).explain(extended=true)
    //spark.time(topChanBySQLbyPeriod(spark, parquetTarget, l))
/**
 *   Build  Weekly purchases Projection within one quarter
 */
val QuarterForCheck  = 4

    csvTarget.createOrReplaceTempView("csvTarget")

    spark.sql("select count(*),  weekofyear(to_date(DATE_FORMAT(purchaseTime, 'dd-MM-yyyy'), 'dd-MM-yyyy')) as weekOfYear " +
      "from csvTarget " +
      " where quarter(to_date(DATE_FORMAT(purchaseTime, 'dd-MM-yyyy'), 'dd-MM-yyyy')) ="+QuarterForCheck+" " +
      " group by weekofyear(to_date(DATE_FORMAT(purchaseTime, 'dd-MM-yyyy'), 'dd-MM-yyyy'))" +
      "order by 2 asc ").show()

  }
}
