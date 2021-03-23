package com.dsm.azure

import com.dsm.utils.Constants
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object TextFile2Df {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Dataframe Example")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)

    println("\nCreating dataframe from CSV file using 'SparkSession.read.format()',")
    val finDf = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .format("csv")
      .load("wasb:///sparksample/posts.csv")

    finDf.printSchema()
    finDf.show()
    
    spark.close()
  }
}
