package com.dsm.dataframe.sql

import com.dsm.model.Product
import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object WindowsFuncDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Windows Function Example")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    val finFilePath = s"s3n://${s3Config.getString("s3_bucket")}/finances-small"
    val financeDf = spark.read.parquet(finFilePath)
    financeDf.createOrReplaceTempView("raw_finances")
    spark.sql("""
      select
        AccountNumber,
        Amount,
        to_date(cast(unix_timestamp(Date, 'MM/dd/yyyy') as timestamp)) as Date,
        Description
      from
        raw_finances
      """)
        .createOrReplaceTempView("finances")

    financeDf.printSchema()

    spark.sql("""
      select
        AccountNumber,
        Amount,
        Date,
        Description,
        avg(Amount) over (partition by AccountNumber order by Date rows between 4 preceding and 0 following) as RollingAvg
      from
        finances
      """)
      .show()

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
    val products = spark.createDataFrame(productList)
    products.createOrReplaceTempView("products")
    products.printSchema()

    spark.sql("""
        select
          product,
          category,
          revenue,
          lag(revenue, 1) over (partition by category order by revenue) as prevRevenue,
          lag(revenue, 2, 0) over(partition by category order by revenue) as prev2Revenue,
          row_number() over (partition by category order by revenue) as row_number,
          rank() over(partition by category order by revenue) as rev_rank,
          dense_rank() over(partition by category order by revenue) as rev_dense_rank
         from
          products
      """)
        .show()

    spark.close()
  }
}
