package com.dsm.dataframe.sql

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object FinanceDataAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Finance Data Analysis")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    val finFilePath = s"s3n://${s3Config.getString("s3_bucket")}/finances-small"
    val financeDf = spark.sql(s"select * from parquet.`$finFilePath`")

    financeDf.printSchema()
    financeDf.show()
    financeDf.createOrReplaceTempView("finances")

    spark.sql("select * from finances order by amount").show(5)

    spark.sql("select concat_ws(' - ', AccountNumber, Description) as AccountDetails from finances")
      .show(5, false)

    val aggFinanceDf = spark.sql("""
      select
        AccountNumber,
        sum(Amount) as TotalTransaction,
        count(Amount) as NumberOfTransaction,
        max(Amount) as MaxTransaction,
        min(Amount) as MinTransaction,
        collect_set(Description) as UniqueTransactionDescriptions
      from
       finances
      group by
       AccountNumber
       """)
    aggFinanceDf.show(false)
    aggFinanceDf.createOrReplaceTempView("agg_finances")

    print("Read SQL query from application.conf file,")
    spark.sql(rootConfig.getConfig("spark_sql_demo").getString("agg_demo"))
      .show(false)

    val compDf = spark.read
      .json("s3n://" + Constants.S3_BUCKET + "/company.json")
    compDf.createOrReplaceTempView("companies")
    compDf.show(false)
    compDf.printSchema()

    val employeeDfTemp = spark.sql("select company, explode(employees) as employee from companies")
    employeeDfTemp.show()
    employeeDfTemp.createOrReplaceTempView("employees")
    val employeeDfTemp2 = spark.sql("select company, posexplode(employees) as (employeePosition, employee) from companies")
    employeeDfTemp2.show()
    spark.sql(rootConfig.getConfig("spark_sql_demo").getString("case_when_demo"))
    .show(false)

    spark.close()
  }
}
