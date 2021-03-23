package assignment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val df = spark.read
      .json("/Users/sidhartharay/Documents/datasets/yelp_academic_dataset_business.json")

    // df.printSchema()
    // df.show(5)

    // You are asked by the CEO to provide a report with the following data:
    //  "business id", "name", "address", "hours", "city", "state", "number of reviews", "stars".
    val reqDf = df.select("business_id", "name", "full_address", "hours", "city", "state", "review_count", "stars")
    //      .show(5, false)
    // reqDf.printSchema
    // reqDf.show(5, false)
    val restaurantsDf = reqDf.select("business_id", "name", "full_address", "city", "state", "review_count", "stars")
    val timingDf = reqDf.select("business_id", "hours")
    timingDf
      .select($"business_id", $"hours.Friday".as("Friday"), $"hours.Monday".as("Monday"))
      .groupBy($"business_id")
      .pivot()
      .show()

    // Next you are asked to identify the number of businesses in the state of Wisconsin
    //    with greater than 20 reviews.
    // df.select("state").distinct().orderBy($"state".desc).show()
    // println("Count = " + df.filter("state = 'WI' and review_count > 20").count())

    // Next you are asked to identify which city has the highest stars (rating) of its businesses?
    // val heighestStars = df.select(max("stars")).rdd.collect().map(rec => rec.getDouble(0)).toList
    // df.select("city","name",  "stars")
    //   .filter("stars = " + heighestStars(0))
    //   .select("city").distinct()
    //   .show()

    // df.select("city","stars", "review_count")
    //  .groupBy("city")
    //  .agg(avg($"stars").as("stars_avg"), sum($"review_count").as("review_count_sum"))
    //  .orderBy($"review_count_sum".desc, $"stars_avg".desc)
    //  .show()

  }
}
