package com.dsm.rdd

import com.dsm.model.{Demographic, Finance}
import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession

/**
  * Count: Swiss students who have debt & financial dependents:
  * 1. Filter down the data set first (look at only people with debt & financial dependents)
  * 2. Filter to select people in Switzerland (look at only people in Switzerland)
  * 3. Inner join on smaller, filtered down data set
  *
  */
object ScholashipRecipientFilterJoin {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local[*]").appName("Dataframe Example").getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", Constants.ACCESS_KEY)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", Constants.SECRET_ACCESS_KEY)

    val demographicsRDD = sparkSession.sparkContext.textFile(s"s3n://${Constants.S3_BUCKET}/demographic.csv")
    val financesRDD = sparkSession.sparkContext.textFile(s"s3n://${Constants.S3_BUCKET}/finances.csv")

    val demographicsPairedRdd = demographicsRDD.map(record => record.split(","))
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
        .map(demographic => (demographic.id, demographic))      //Pair RDD, (id, demographics)
        .filter(p => p._2.country == "Switzerland")

    val financesPairedRdd = financesRDD.map(record => record.split(","))
          .map(record => Finance(
            record(0).toInt,
            record(1).toBoolean,
            record(2).toBoolean,
            record(3).toBoolean,
            record(4).toInt
          )
        )
        .map(finance => (finance.id, finance))                  //Pair RDD, (id, finances)
        .filter(p => p._2.hasFinancialDependents && p._2.hasDebt)

    demographicsPairedRdd.join(financesPairedRdd)
      .foreach(println)

    sparkSession.close()
  }
}