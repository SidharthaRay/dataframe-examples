package com.dsm.dataframe.dsl

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FinanceDataAnalysis {
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

    val finFilePath = s"s3n://${s3Config.getString("s3_bucket")}/finances-small"
//    val finFilePath = "/Users/sidhartha.ray/Documents/workspace/dataframe-examples/src/main/resources/data/finances-small"
    val financeDf = sparkSession.read.parquet(finFilePath)

    financeDf.printSchema()
    financeDf.show()

    financeDf
      .orderBy($"Amount")
      .show(5)

    financeDf
      .select(concat_ws(" - ", $"AccountNumber", $"Description").alias("AccountDetails"))
      .show(5, false)

//    financeDf
//      .withColumn("AccountDetails", concat_ws(" - ", $"AccountNumber", $"Description"))
//      .show(5, false)

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

    val companiesJson = List(
      """{"company":"NewCo","employees":[{"firstName":"Sidhartha","lastName":"Ray"},{"firstName":"Pratik","lastName":"Solanki"}]}""",
      """{"company":"FamilyCo","employees":[{"firstName":"Jiten","lastName":"Pupta"},{"firstName":"Pallavi","lastName":"Gupta"}]}""",
      """{"company":"OldCo","employees":[{"firstName":"Vivek","lastName":"Garg"},{"firstName":"Nitin","lastName":"Gupta"}]}""",
      """{"company":"ClosedCo","employees":[]}"""
    )
    val companiesRDD = sparkSession.sparkContext.makeRDD(companiesJson)
    val companiesDF = sparkSession.read.json(companiesRDD)
    companiesDF.show(false)
    companiesDF.printSchema()

    val employeeDfTemp = companiesDF.select($"company", explode($"employees").as("employee"))
    employeeDfTemp.show()
    val employeeDfTemp2 = companiesDF.select($"company", posexplode($"employees").as(Seq("employeePosition", "employee")))
    employeeDfTemp2.show()
    val employeeDf = employeeDfTemp.select($"company", expr("employee.firstName as firstName"))
    employeeDf.select($"*",
      when($"company" === "FamilyCo", "Premium")
        .when($"company" === "OldCo", "Legacy")
        .otherwise("Standard").as("Tier"))
      .show(false)

    sparkSession.close()
  }
}
