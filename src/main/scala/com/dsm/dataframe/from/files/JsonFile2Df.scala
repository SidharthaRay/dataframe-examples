package com.dsm.dataframe.from.files

import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JsonFile2Df {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Dataframe Example")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", Constants.ACCESS_KEY)
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", Constants.SECRET_ACCESS_KEY)

    import spark.implicits._
    val compDf = spark.read
      .json("s3n://" + Constants.S3_BUCKET + "/company.json")
    compDf.printSchema()
    compDf.show(false)

    val flattenedDf = compDf.select($"company", explode($"employees").alias("employee"))
    flattenedDf.show(false)

    flattenedDf.select($"company", $"employee.firstName".alias("emp_name"))
      .show(false)

    spark.close()
  }
}
