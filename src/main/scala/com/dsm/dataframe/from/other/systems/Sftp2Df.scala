package com.dsm.dataframe.from.other.systems

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

object Sftp2Df {
  def main(args: Array[String]): Unit = {
    try {
      val sparkSession = SparkSession.builder.master("local[*]").appName("Dataframe Example").getOrCreate()
      sparkSession.sparkContext.setLogLevel(Constants.ERROR)

      val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
      val sftpConfig = rootConfig.getConfig("sftp_conf")

      val olTxnDf = sparkSession.read.
        format("com.springml.spark.sftp").
        option("host", sftpConfig.getString("hostname")).
        option("port", sftpConfig.getString("port")).
        option("username", sftpConfig.getString("username")).
//        option("password", "Temp1234").
        option("pem", sftpConfig.getString("pem")).
        option("fileType", "csv").
        option("delimiter", "|").
        load(s"${sftpConfig.getString("directory")}/receipts_delta_GBR_14_10_2017.csv")

      olTxnDf.show()
      sparkSession.close()
    } catch {
      case ex: Throwable => {
        ex.printStackTrace()
      }
    }
  }
}
