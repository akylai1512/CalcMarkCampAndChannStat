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

import java.util.Date

object CalcBySparkApi {

  def topCampBySparkApi( df: DataFrame): DataFrame = {
    df.groupBy("campaignId")
      .agg(functions.sum("billingCost").as("sumBillingCost"))
      .orderBy(desc("sumBillingCost")).filter(col("isConfirmed") === "True").limit(10)
  }

  def popChanalSparkApi( df: DataFrame): DataFrame = {
    df.groupBy("campaignId", "channelIid")
      .agg(functions.count("channelIid").as("countBillingCost"))
      .withColumn("row_number", row_number.over(Window.partitionBy("campaignId").orderBy(col("countBillingCost").desc)))
      .select(col("campaignId"),
              col("channelIid"),
              col("countBillingCost"))
      .filter(col("row_number") === 1).orderBy(desc("countBillingCost"))
  }
}