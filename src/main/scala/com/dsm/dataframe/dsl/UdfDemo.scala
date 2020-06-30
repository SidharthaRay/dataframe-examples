package com.dsm.dataframe.dsl

import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UdfDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("UDF Demo")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)
    import spark.implicits._

    val sampleDf = spark.createDataFrame(
      List((1, "This is some sample data"), (2, "and even more."))
    ).toDF("id", "text")

    val capitalizerUDF = spark.udf
      .register("capitalizeFirstUsingSpace", (fullString: String) => fullString.split(" ").map(_.capitalize).mkString(" "))
    sampleDf.select($"id", callUDF("capitalizeFirstUsingSpace", $"text").as("text")).show(false)

    val capitalizerUdf = udf((fullString: String, splitter: String) => fullString.split(splitter).map(_.capitalize).mkString(splitter))
    sampleDf.select($"id", capitalizerUdf($"text", lit(" ")).as("text")).show(false)

    spark.close()
  }
}
