package com.dsm.dataframe.dsl

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FinanceDataAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Finance Data Analysis")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)
    import spark.implicits._

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    val finFilePath = s"s3n://${s3Config.getString("s3_bucket")}/finances-small"
    val financeDf = spark.read.parquet(finFilePath)

    financeDf.printSchema()
    financeDf.show()

    financeDf
      .orderBy($"Amount")
      .show(5)

    financeDf
      .select(concat_ws(" - ", $"AccountNumber", $"Description").alias("AccountDetails"))
      .show(5, false)

    val aggFinanceDf = financeDf
        .groupBy($"AccountNumber")
        .agg(avg($"Amount").as("AverageTransaction"),
          sum($"Amount").as("TotalTransaction"),
          count($"Amount").as("NumberOfTransaction"),
          max($"Amount").as("MaxTransaction"),
          min($"Amount").as("MinTransaction"),
          collect_set($"Description").as("UniqueTransactionDescriptions")
        )
    aggFinanceDf.show(false)

    aggFinanceDf.select(
      $"AccountNumber",
      $"UniqueTransactionDescriptions",
      size($"UniqueTransactionDescriptions").as("CountOfUniqueTransactionTypes"),
      sort_array($"UniqueTransactionDescriptions", false).as("OrderedUniqueTransactionDescriptions"),
      array_contains($"UniqueTransactionDescriptions", "Movies").as("WentToMovies")
    )
    .show(false)

    val compDf = spark.read
      .json("s3n://" + Constants.S3_BUCKET + "/company.json")
    compDf.show(false)
    compDf.printSchema()

    val employeeDfTemp = compDf.select($"company", explode($"employees").as("employee"))
    employeeDfTemp.show()
    val employeeDfTemp2 = compDf.select($"company", posexplode($"employees").as(Seq("employeePosition", "employee")))
    employeeDfTemp2.show()
    val employeeDf = employeeDfTemp.select($"company", expr("employee.firstName as firstName"))
    employeeDf.select($"*",
      when($"company" === "FamilyCo", "Premium")
        .when($"company" === "OldCo", "Legacy")
        .otherwise("Standard").as("Tier"))
      .show(false)

    spark.close()
  }
}
