package com.dsm.dataframe.dsl

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lag, rank, row_number, _}
import org.apache.spark.sql.expressions.Window
import com.dsm.model.Product

object WindowFuncDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataframe Example")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)
    import sparkSession.implicits._

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

//    val finFilePath = s"s3n://${s3Config.getString("s3_bucket")}/finances-small"
    val finFilePath = s"/Users/sidhartha.ray/Documents/workspace/dataframe-examples/src/main/resources/data/finances-small/"
    val financeDf = sparkSession.read.parquet(finFilePath)
    financeDf.printSchema()

    val accNumPrev4WindowSpec = Window.partitionBy($"AccountNumber")
        .orderBy($"Date")
        .rowsBetween(-4, 0)
    financeDf
      .withColumn("Date", to_date(from_unixtime(unix_timestamp($"Date", "MM/dd/yyyy"))))
      .withColumn("RollingAvg", avg($"Amount").over(accNumPrev4WindowSpec))
      .show(false)

    val productList = List(
      Product("Thin", "Cell phone", 6000),
      Product("Normal", "Tablet", 1500),
      Product("Mini", "Tablet", 5500),
      Product("Ultra Thin", "Cell phone", 5000),
      Product("Very Thin", "Cell phone", 6000),
      Product("Big", "Tablet", 2500),
      Product("Bendable", "Cell phone", 3000),
      Product("Foldable", "Cell phone", 3000),
      Product("Pro", "Tablet", 4500),
      Product("Pro2", "Tablet", 6500)
    )
    val products = sparkSession.createDataFrame(productList)
    products.printSchema()

    val catRevenueWindowSpec = Window.partitionBy($"category")
      .orderBy($"revenue")
    products
      .select($"product",
        $"category",
        $"revenue",
        lag($"revenue", 1).over(catRevenueWindowSpec).as("prevRevenue"),
        lag($"revenue", 2, "N/A").over(catRevenueWindowSpec).as("prev2Revenue"),
        row_number().over(catRevenueWindowSpec).as("row_number"),
        rank().over(catRevenueWindowSpec).as("rev_rank"),
        dense_rank().over(catRevenueWindowSpec).as("rev_dense_rank")
      )
      .show()

/*
    financeDf
      .select($"*", window($"Date", "30 days", "15 minutes"))
      .show()
*/


    sparkSession.close()
  }
}
