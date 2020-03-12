package com.dsm.dataframe.from.files

import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JsonFile2Df {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local[2]").appName("Dataframe Example").getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", Constants.ACCESS_KEY)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", Constants.SECRET_ACCESS_KEY)

    import sparkSession.implicits._
    val employeeDf = sparkSession.read
      .json("s3n://" + Constants.S3_BUCKET + "/cart_sample_small.txt")

    employeeDf.printSchema()
    employeeDf.show(false)

    employeeDf.select($"cart.swid".alias("cust_id")).show(false)
    employeeDf.select(explode($"cart.vacationOffer.package.room").alias("vacation_room")).show(false)

    sparkSession.close()
  }
}
