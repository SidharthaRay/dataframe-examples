package dsm.ex

import com.dsm.utils.Constants
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Rdd2DfThruExplicitSchema {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local[*]").appName("RDD to Dataframe through explicit schema specification").getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", Constants.ACCESS_KEY)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", Constants.SECRET_ACCESS_KEY)

    println("\nConvert RDD to Dataframe using SparkSession.createDataframe(),")
    // Creating RDD of Row
    val txnFctRdd = sparkSession.sparkContext.textFile("s3n://" + Constants.S3_BUCKET + "/txn_fct.csv")//("/Users/sidhartha.ray/Documents/workspace/dataframe-examples/src/main/resources/data/txn_fct.csv")
      .filter(record => !record.contains("txn_id"))
      .map(record => record.split("\\|"))
      .map(record => Row(record(0).toLong,
        record(1).toLong,
        record(2).toDouble,
        record(3).toLong,
        record(4).toInt,
        record(5).toLong,
        record(6))
      )

    // Creating the schema
    val txnFctSchema = StructType(
      StructField("txn_id", LongType, false) ::
      StructField("created_time_str", LongType, false) ::
      StructField("amount", DoubleType, true) ::
      StructField("cust_id", LongType, true) ::
      StructField("status", IntegerType, true) ::
      StructField("merchant_id", LongType, true) ::
      StructField("created_time_ist", StringType, true) :: Nil
    )

    var txnFctDf = sparkSession.createDataFrame(txnFctRdd, txnFctSchema)
    txnFctDf.printSchema()
    txnFctDf.show(5, false)

    // Applying tranformation on dataframe using DSL (Domain Specific Language)
    txnFctDf = txnFctDf
      .withColumn("created_time_ist", unix_timestamp(txnFctDf("created_time_ist"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType))
    txnFctDf.printSchema()
    txnFctDf.show(5, false)

    println("# of records = " + txnFctDf.count())
    println("# of merchants = " + txnFctDf.select("merchant_id").distinct().count())

    var txnAggDf = txnFctDf
      .repartition(10, txnFctDf("merchant_id"))
      .groupBy("merchant_id")
      .agg("amount" -> "sum", "status" -> "approx_count_distinct")

    txnAggDf.show(5, false)

    txnAggDf = txnAggDf
      .withColumnRenamed("sum(amount)", "total_amount")
      .withColumnRenamed("approx_count_distinct(status)", "dist_status_count")
    txnAggDf.show(5, false)

    sparkSession.close()
  }
}
