package com.dsm.dataframe.from.rdd

import com.dsm.utils.Constants
import org.apache.spark.sql.SparkSession

object Rdd2DfThruSchemaAutoInfer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("RDD to Dataframe")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", Constants.ACCESS_KEY)
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", Constants.SECRET_ACCESS_KEY)

    val txnFctRdd = spark.sparkContext.textFile("s3n://" + Constants.S3_BUCKET + "/txn_fct.csv")//("/Users/sidhartha.ray/Documents/workspace/dataframe-examples/src/main/resources/data/txn_fct.csv")
      .filter(record => !record.contains("txn_id"))
      .map(record => record.split("\\|"))
      .map(record => (record(0).toLong,
        record(1).toLong,
        record(2).toDouble,
        record(3).toLong,
        record(4).toInt,
        record(5).toLong,
        record(6))
      )

    import spark.sqlContext.implicits._

    println("\nConvert RDD to Dataframe using toDF() - without column names,")
    //println("\nConvert RDD to Dataframe using toDF() - without column names,")
    val txnDfNoColNames = txnFctRdd.toDF
    txnDfNoColNames.printSchema()
    txnDfNoColNames.show(5, false)

    println("\nConvert RDD to Dataframe using toDF(colNames: String*) - with column names,")
    val txnDfWithColName = txnFctRdd.toDF("txn_id",
      "created_time_string",
      "amount",
      "cust_id",
      "status",
      "merchant_id",
      "created_time_ist"
    )
    txnDfWithColName.printSchema()
    txnDfWithColName.show()

    println("\nCreating Dataframe out of RDD without column names using createDataframe(),")
    val txnDfNoColNames2 = spark.createDataFrame(txnFctRdd)
    txnDfNoColNames2.printSchema()
    txnDfNoColNames2.show(5)

    spark.close()
  }
}
