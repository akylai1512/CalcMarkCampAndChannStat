package org.example

import munit.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.example.CalcBySQL.topCampBySQL

class testCalcBySql extends FunSuite {
  val spark = SparkSession
    .builder
    .appName("Apache Spark")
    .master("local")
    .getOrCreate()


  spark.sparkContext.setLogLevel("ERROR")
  val csvTarget =spark.read.format("csv").options(Map("inferSchema" -> "true", "sep" -> ",", "header" -> "true")).csv("src/resources/capstone-dataset/csv_dataset/targetTable.csv/*")

  test("is it top 10 ") {
    assert( topCampBySQL(spark ,csvTarget ).count()==10)
  }
  test("max first place summ should be 2041060.8399999978") {
    assert( topCampBySQL(spark ,csvTarget ).count()==10)
  }


}
