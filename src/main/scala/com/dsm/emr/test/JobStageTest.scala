package com.dsm.emr.test

import org.apache.spark.sql.SparkSession

object JobStageTest {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "/")
    val sparkSession = SparkSession.builder().appName("UPS Source Data Loading").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    val rdd1 = sparkSession.sparkContext
      .textFile("s3n://ups-regen/auto_test/sample_data")
      .map(record => record.split("~", -1))
      .filter(record => !((record(0) == null && record(13) == null) || (record(0).isEmpty && record(13).isEmpty)))
      .map(record => (record(0), (record(13).toDouble, 1)))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v2._2 + v2._2))
      .filter(record => record._1 != null)
      .mapValues(record => math.round(record._1 / record._2))
      .mapValues(record => (record, if(record < 5000.0) "Tier1" else "Tier2"))
      .map(record => (record._2._2, record._2._1))
      .reduceByKey((v1, v2) => v1 + v2)
      .mapValues(record => math.round(record))
      .take(5)
      .foreach(println)

    val rdd2 = sparkSession.sparkContext
      .textFile("s3n://ups-regen/auto_test/sample_data")
      .map(record => record.split("~", -1))
      .filter(record => !((record(0) == null && record(13) == null) || (record(0).isEmpty && record(13).isEmpty)))
      .map(record => (record(0), (record(13).toDouble, 1)))
      .reduceByKey((v1, v2) => (v1._1 + v2._1, v2._2 + v2._2))
      .filter(record => record._1 != null)
      .mapValues(record => math.round(record._1 / record._2))
      .mapValues(record => (record, if(record < 5000.0) "Tier1" else "Tier2"))
      .map(record => (record._2._2, record._2._1))
      .reduceByKey((v1, v2) => v1 + v2)
      .mapValues(record => math.round(record))
      .take(5)
      .foreach(println)

//    val rdd3 = join radd1 & rdd2

    sparkSession.close()
  }
}
