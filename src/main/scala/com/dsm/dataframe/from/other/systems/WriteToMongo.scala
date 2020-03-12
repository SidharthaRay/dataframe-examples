package com.dsm.dataframe.from.other.systems

import com.dsm.model.Student
import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object WriteToMongo {
  def main(args: Array[String]): Unit = {
    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val mongoConfig = rootConfig.getConfig("mongodb_config")

    val sparkSession = SparkSession.builder.master("local[*]").appName("Dataframe Example")
      .config("spark.mongodb.input.uri", mongoConfig.getString("input.uri"))
      .config("spark.mongodb.output.uri", mongoConfig.getString("output.uri"))
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)

    val students = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(
        List(
          Student("Sidhartha", "Ray", "ITER", 200),
          Student("Satabdi", "Ray", "CET", 100)
        )
      )
    )
    students.show()
    students
      .write
      .format("com.mongodb.spark.sql.DefaultSource")
      .mode("append")
      .option("database", mongoConfig.getString("input.database"))
      .option("collection", mongoConfig.getString("collection"))
      .save()

    sparkSession.close()
  }

}
