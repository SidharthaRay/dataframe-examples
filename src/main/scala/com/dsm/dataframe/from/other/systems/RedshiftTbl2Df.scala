package com.dsm.dataframe.from.other.systems

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object RedshiftTbl2Df {
  def main(args: Array[String]): Unit = {
    try {
      val sparkSession = SparkSession.builder.master("local[*]").appName("Dataframe Example").getOrCreate()
      sparkSession.sparkContext.setLogLevel(Constants.ERROR)

      val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
      val s3Config = rootConfig.getConfig("s3_conf")
      val redshiftConfig = rootConfig.getConfig("redshift_conf")

      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
      sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

      println("Reading txn_fact table from AWS Redshift and creating Dataframe,")
      val jdbcUrl = Constants.getRedshiftJdbcUrl(redshiftConfig)
      val s3Bucket = s3Config.getString("s3_bucket")
      val txnDf = sparkSession.sqlContext.read
        .format("com.databricks.spark.redshift")
        .option("url", jdbcUrl)
        .option("tempdir", s"s3n://${s3Bucket}/temp")
        .option("forward_spark_s3_credentials", "true")
        .option("dbtable", "PUBLIC.TXN_FCT")
        .load()

      txnDf.show()

      sparkSession.close()
    } catch {
      case ex: Throwable => {
        ex.printStackTrace()
      }
    }

  }

}
