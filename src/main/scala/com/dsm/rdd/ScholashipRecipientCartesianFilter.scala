package com.dsm.rdd

import com.dsm.model.{Demographic, Finance}
import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession

/**
  * Count: Swiss students who have debt & financial dependents:
  * 1. Cartesian product on both the datasets
  * 2. Filter to select resulting of cartesian with same IDs
  * 3. Filter to select people in Switzerland who have debt and financial dependents
  *
  */
object ScholashipRecipientCartesianFilter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Scholaship Recipient Cartesian -> Filter").getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", Constants.ACCESS_KEY)
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", Constants.SECRET_ACCESS_KEY)

    val demographicsRDD = spark.sparkContext.textFile(s"s3n://${Constants.S3_BUCKET}/demographic.csv")
    val financesRDD = spark.sparkContext.textFile(s"s3n://${Constants.S3_BUCKET}/finances.csv")

    val demographicsPairedRdd = demographicsRDD
      .map(record => record.split(","))
      .map(record =>
        Demographic(record(0).toInt,
          record(1).toInt,
          record(2).toBoolean,
          record(3),
          record(4),
          record(5).toBoolean,
          record(6).toBoolean,
          record(7).toInt
        )
      )
      .map(demographic => (demographic.id, demographic))
      //Pair RDD, (id, demographics)

    val financesPairedRdd = financesRDD
      .map(record => record.split(","))
      .map(record => Finance(
          record(0).toInt,
          record(1).toBoolean,
          record(2).toBoolean,
          record(3).toBoolean,
          record(4).toInt
        )
      )
      .map(finance => (finance.id, finance))
      //Pair RDD, (id, finances)

    demographicsPairedRdd.cartesian(financesPairedRdd)
      .filter{
        case (dem, fin) => dem._1 == fin._1
      }
      .filter{
        case(dem, fin) =>
          (dem._2.country == "Switzerland") && (fin._2.hasDebt) && (fin._2.hasFinancialDependents)
      }
      .foreach(println)

    spark.close()
  }
}