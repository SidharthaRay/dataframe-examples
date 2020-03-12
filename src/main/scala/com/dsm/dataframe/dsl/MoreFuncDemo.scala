package com.dsm.dataframe.dsl

import com.dsm.model.Person
import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MoreFuncDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataframe Example")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel(Constants.ERROR)
    import sparkSession.implicits._

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    val peopleDf = sparkSession.createDataFrame(List(
      Person("Sidhartha", "Ray", 32, None, Some("Programmer")),
      Person("Pratik", "Solanki", 22, Some(176.7), None),
      Person("Ashok ", "Pradhan", 62, None, None),
      Person(" ashok", "Pradhan", 42, Some(125.3), Some("Chemical Engineer")),
      Person("Pratik", "Solanki", 22, Some(222.2), Some("Teacher"))
    ))

    peopleDf.show()
    peopleDf.groupBy($"firstName").agg(first($"weightInLbs")).show()
//    peopleDf.groupBy(trim(lower($"firstName"))).agg(first($"weightInLbs")).show()
//    peopleDf.groupBy(trim(lower($"firstName"))).agg(first($"weightInLbs", true)).show()
//    peopleDf.sort($"weightInLbs".desc).groupBy(trim(lower($"firstName"))).agg(first($"weightInLbs", true)).show()
//    peopleDf.sort($"weightInLbs".asc_nulls_last).groupBy(trim(lower($"firstName"))).agg(first($"weightInLbs", true)).show()

    var correctedPeopleDf = peopleDf
      .withColumn("firstName", initcap($"firstName"))
      .withColumn("firstName", ltrim(initcap($"firstName")))
      .withColumn("firstName", trim(initcap($"firstName")))
    correctedPeopleDf.groupBy($"firstName").agg(first($"weightInLbs")).show()

    correctedPeopleDf = correctedPeopleDf
        .withColumn("fullName", format_string("%s %s", $"firstName", $"lastName"))
    correctedPeopleDf.show()

    correctedPeopleDf = correctedPeopleDf
        .withColumn("weightInLbs", coalesce($"weightInLbs", lit(0)))
    correctedPeopleDf.show()

    correctedPeopleDf
        .filter(lower($"jobType").contains("engineer"))
        .show()

    correctedPeopleDf
      .filter(lower($"jobType").isin(List("chemical engineer", "teacher"):_*))
      .show()

    sparkSession.close()
  }
}
