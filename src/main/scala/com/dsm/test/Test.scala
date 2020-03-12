package com.dsm.test

import org.apache.spark.sql.{SaveMode, SparkSession}

object Test {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("Spark Test").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    val events = List(
      "Prime Sound,10000",
      "Prime Sound,5000",
      "Sportsorg,20000",
      "Sportsorg,5000",
      "Ultra Sound,30000",
      "Ultra Sound,5000"
    )

    val eventsRDD = sparkSession.sparkContext.parallelize(events)
    eventsRDD.map(event => event.split(","))
      .map(event => (event(0), (event(1).toDouble)))
      .reduceByKey((budget1, budget2) => if(budget1 < budget2) budget1 else budget2)
      .foreach(println)


/*
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIA4TAG5FQOEYXMSJ73")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")

    val rdd = sparkSession.sparkContext.textFile(path = "s3n://airflowdemo99/KC_Extract_1_20171009.csv")
    //    rdd.map(x => x.split(",")).map(r => (r(0),r(1))).reduceByKey((x,y) => (x+y)).take(3).foreach(println)

    rdd.take(3).foreach(println)
*/
    sparkSession.close()
  }
}
