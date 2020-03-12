package com.dsm.dataframe.dsl

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UdfDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataframe Example")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)
    import sparkSession.implicits._

    val sampleDf = sparkSession.createDataFrame(
      List((1, "This is some sample data"), (2, "and even more."))
    ).toDF("id", "text")

    val capitalizerUDF = sparkSession.udf
      .register("capitalizeFirstUsingSpace", (fullString: String) => fullString.split(" ").map(_.capitalize).mkString(" "))
    sampleDf.select($"id", callUDF("capitalizeFirstUsingSpace", $"text").as("text")).show(false)

    val capitalizerUdf = udf((fullString: String, splitter: String) => fullString.split(splitter).map(_.capitalize).mkString(splitter))
    sampleDf.select($"id", capitalizerUdf($"text", lit(" ")).as("text")).show(false)

    sparkSession.close()
  }
}
