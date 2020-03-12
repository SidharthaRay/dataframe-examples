package com.dsm.dataframe.from.other.systems

import com.dsm.utils.Constants
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Write2Redshift {
  def main(args: Array[String]): Unit = {
    try {
      val sparkSession = SparkSession.builder.master("local[*]").appName("Write dataframe to Redshift Example").getOrCreate()
      sparkSession.sparkContext.setLogLevel(Constants.ERROR)

      val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
      val s3Config = rootConfig.getConfig("s3_conf")
      val redshiftConfig = rootConfig.getConfig("redshift_conf")

      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

      println("\nCreating Dataframe from txn_fact dataset,")
      val s3Bucket = s3Config.getString("s3_bucket")
      val txnDf = sparkSession.read
        .option("header","true")
        .option("delimiter", "|")
        .csv(s"s3n://${s3Bucket}/txn_fct.csv")

      txnDf.show()

      println("Writing txn_fact dataframe to AWS Redshift Table   >>>>>>>")
      val jdbcUrl = Constants.getRedshiftJdbcUrl(redshiftConfig)
      txnDf.write
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("tempdir", s"s3n://${s3Bucket}/temp")
        .option("forward_spark_s3_credentials", "true")
        .option("dbtable", "PUBLIC.TXN_FCT")
        .mode(SaveMode.Overwrite)
        .save()
      println("Completed   <<<<<<<<<")

      sparkSession.close()
    }  catch{
      case ex: Throwable => {
        ex.printStackTrace()
      }
    }

  }
}
