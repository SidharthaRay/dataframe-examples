package com.dsm.dataframe.dsl

import com.dsm.model._
import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinsDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Joins Example")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Constants.ERROR)

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val s3Config = rootConfig.getConfig("s3_conf")

    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3Config.getString("access_key"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3Config.getString("secret_access_key"))

    val employeeDf = spark.createDataFrame(List(
      Employee(1, "Sidhartha", "Ray"),
      Employee(2, "Pratik", "Solanki"),
      Employee(3, "Ashok", "Pradhan"),
      Employee(4, "Rohit", "Bhangur"),
      Employee(5, "Kranti", "Meshram"),
      Employee(7, "Ravi", "Kiran")
    ))

    val empRoleDf = spark.createDataFrame(List(
      Role(1, "Architect"),
      Role(2, "Programmer"),
      Role(3, "Analyst"),
      Role(4, "Programmer"),
      Role(5, "Architect"),
      Role(6, "CEO")
    ))

//    employeeDf.join(empRoleDf, $"id" === $"id").show(false)   // Ambiguous column name "id"
    employeeDf.join(empRoleDf, employeeDf("id") === empRoleDf("id")).show(false)
    employeeDf.join(broadcast(empRoleDf), employeeDf("id") === empRoleDf("id")).show(false)

    employeeDf.join(empRoleDf, Seq("id"), "inner").show(false)    //"left_outer"/"left", "full_outer"/"full"/"outer"
    employeeDf.join(empRoleDf, Seq("id"), "right_outer").show(false)    //"left"
    employeeDf.join(empRoleDf, Seq("id"), "left_anti").show(false)

    employeeDf.join(empRoleDf, employeeDf("id") === empRoleDf("id"), "cross").show(false)  // cross join

  }
}
