package com.dsm.optional

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object AvroTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("Avro Test").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    sparkSession.sqlContext
      .sparkContext
      .hadoopConfiguration
      .set("avro.mapred.ignore.inputs.without.extension", "true")

    val personDf = sparkSession.read
      .format("com.databricks.spark.avro")
      .load("/Users/sidhartha.ray/Documents/AvroTest/livePerson.avro")

//    val cnt = personDf.select(explode(personDf("recordCollection.body.member1.header"))).count()
//    println("Count " + cnt)
//    val df = personDf.select(explode(personDf("recordCollection.body.member1.header")).as("header"))//.show(false)
//    df.printSchema()
//    df.select("header.visitId").show(false)
    for(i <- 1 to 10) {
      val df2 = personDf.select(explode(personDf("recordCollection.body.member" + i + ".header")).as("header")) //.show(false)
//      df2.printSchema()
      println("Count = " + df2.count())
    }

//    personDf.select("recordCollection.body.member1.header").show(false)
//    personDf.select("recordCollection.body.member2").show(1, false)
//    personDf.select("recordCollection.body.member3").show(1, false)

//    println("Count: " + personDf.count()) //298
//    personDf.printSchema()

//    personDf.select($"dataType",
//      $"metaData.accountId".as("accountId"))
//      .show()

//    val recCol = personDf.select(explode($"recordCollection"))
//    recCol.printSchema()
//    recCol.show(3, false)

//    personDf.rdd.take(2).foreach(println)
//    personDf.rdd.map(rec => rec(0)).take(2).foreach(println)
//    personDf.rdd.map(rec => parse(rec)).take(2).foreach(println)

//    personDf.rdd.map(parse())
    sparkSession.close()
  }

//  def parse(record: Row): Person = {
//    val person = new Person()
//
//    person.dataType = record(0).toString
//
//    val metadata = record.getStruct(1)//.asInstanceOf[List[Any]]
//    person.mdAccountId = metadata.getAs[String](0)
//    person.mdSchemaVersion = metadata.getAs[String](1)
//    person.mdstartTime = metadata.getAs[Long](2)
//    person.mdEndTime = metadata.getAs[Long](3)

//    val recordCollection = record.getAs[mutable.WrappedArray[StructType[StructType[ArrayType]]]](2)
//    val recordCollection = record(2).asInstanceOf[mutable.WrappedArray[List[Any]]]
//    println(recordCollection(0))
//    recordCollection.map(rec => rec.asInstanceOf[StructType]).foreach(println)

//    println(person)
//    person
//  }
}
