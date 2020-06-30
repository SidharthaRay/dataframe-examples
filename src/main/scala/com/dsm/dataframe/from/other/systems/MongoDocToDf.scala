package com.dsm.dataframe.from.other.systems

import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

object MongoDocToDf {
  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val mongoConfig = rootConfig.getConfig("mongodb_config")

    val spark = SparkSession.builder.master("local[*]").appName("Mongo Doc To Dataframe")
      .config("spark.mongodb.input.uri", mongoConfig.getString("input.uri"))
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)

    val students = spark.read
      .format("com.mongodb.spark.sql.DefaultSource")
      .option("database", mongoConfig.getString("input.database"))
      .option("collection", mongoConfig.getString("collection"))
      .load()
    students.show()

    spark.close()
  }
}
