package org.example

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.util.Locale


object  CalcBySQL {


  def topCampBySQL(spark: SparkSession, df: DataFrame): DataFrame = {

    df.createOrReplaceTempView("df")

    spark.sql("select d.campaignId, " +
      "sum(billingCost) as sumBillCol " +
      "from df d  " +
      "where lower(d.isConfirmed)= 'true'" +
      "group by d.campaignId order by sum(billingCost) desc limit 10")
  }


  def topChanBySQL(spark: SparkSession, df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("df")

    spark.sql("select * from (SELECT ROW_NUMBER() OVER(PARTITION BY l.campaignId  ORDER BY  l.countBillingCost desc) as rowNumber ," +
      "l.campaignId, l.channelIid, l.countBillingCost " +
      "FROM (SELECT s.campaignId, s.channelIid, COUNT(s.channelIid) as countBillingCost FROM df  s GROUP BY s.campaignId, s.channelIid) as l )" +
      "where rowNumber =1")
  }

  def topCompBySQLbyPeriod(spark :SparkSession, dfs:DataFrame, s:LocalDate):DataFrame ={

    dfs.createOrReplaceTempView("dfs")

    spark.sql("select d.campaignId, " +
      "sum(billingCost) as sumBillCol " +
      "from dfs d  " +
      "where lower(d.isConfirmed)= 'true' " +
      " and DAY('"+s+"') =d.day "+
      " and MONTH('"+s+"') =d.month "+
      " and YEAR('"+s+"')=d.year "+
      " group by d.campaignId order by sum(billingCost) desc limit 10")
  }

  def topChanBySQLbyPeriod(spark :SparkSession, df:DataFrame, s:LocalDate):DataFrame ={

    df.createOrReplaceTempView("df")

    spark.sql("select * from (SELECT ROW_NUMBER() OVER(PARTITION BY l.campaignId  ORDER BY  l.countBillingCost desc) as rowNumber ," +
      "l.campaignId, l.channelIid, l.countBillingCost " +
      "FROM (SELECT s.campaignId, s.channelIid, COUNT(s.channelIid) as countBillingCost " +
      "FROM df  s " +
      "where  DAY('"+s+"') =s.day "+
      " and MONTH('"+s+"') =s.month "+
      " and YEAR('"+s+"')=s.year "+
      "GROUP BY s.campaignId, s.channelIid) as l )" +
      "where rowNumber =1")
  }
}
