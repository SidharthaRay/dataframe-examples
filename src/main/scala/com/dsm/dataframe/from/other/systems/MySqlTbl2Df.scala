package com.dsm.dataframe.from.other.systems

import com.dsm.utils.Constants
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

object MySqlTbl2Df {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local[*]").appName("Dataframe Example").getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val mysqlConfig = rootConfig.getConfig("mysql_conf")

    var jdbcParams = Map("url" ->  getMysqlJdbcUrl(mysqlConfig),
      "lowerBound" -> "1",
      "upperBound" -> "100",
      "dbtable" -> "testdb.TRANSACTIONSYNC",
//      "dbtable" -> "(select a, b, c from testdb.TRANSACTIONSYNC where some_cond) as t",
      "numPartitions" -> "2",
      "partitionColumn" -> "App_Transaction_Id",
      "user" -> mysqlConfig.getString("username"),
      "password" -> mysqlConfig.getString("password")
    )

    println("\nReading data from MySQL DB using SparkSession.read.format(),")
    val txnDF = sparkSession
      .read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .options(jdbcParams)                                                  // options can pass map
      .load()
    txnDF.show()

    sparkSession.stop()
  }

  // Creating Redshift JDBC URL
  def getMysqlJdbcUrl(mysqlConfig: Config): String = {
    val host = mysqlConfig.getString("hostname")
    val port = mysqlConfig.getString("port")
    val database = mysqlConfig.getString("database")
    s"jdbc:mysql://$host:$port/$database?autoReconnect=true&useSSL=false"
  }

}