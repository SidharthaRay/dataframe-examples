package com.dsm.dataframe.sql

import com.dsm.utils.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FinanceDataAnalysis {
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

    val finFilePath = s"s3n://${s3Config.getString("s3_bucket")}/finances-small"
    val financeDf = sparkSession.sql(s"select * from parquet.`$finFilePath`")

    financeDf.printSchema()
    financeDf.show()
    financeDf.createOrReplaceTempView("finances")

    sparkSession.sql("select * from finances order by amount").show(5)

    sparkSession.sql("select concat_ws(' - ', AccountNumber, Description) as AccountDetails from finances").show(5, false)

    val aggFinanceDf = sparkSession.sql("""
      select
        AccountNumber,
        sum(Amount) as TotalTransaction,
        count(Amount) as NumberOfTransaction,
        max(Amount) as MaxTransaction,
        min(Amount) as MinTransaction,
        collect_set(Description) as UniqueTransactionDescriptions
      from
       finances
      group by
       AccountNumber
       """)
    aggFinanceDf.show(false)
    aggFinanceDf.createOrReplaceTempView("agg_finances")

    sparkSession.sql(
      """
        select
          AccountNumber,
          UniqueTransactionDescriptions,
          sort_array(UniqueTransactionDescriptions, false) as OrderedUniqueTransactionDescriptions,
          size(UniqueTransactionDescriptions) as CountOfUniqueTransactionTypes,
          array_contains(UniqueTransactionDescriptions, 'Movies') as WentToMovies
        from
          agg_finances
      """
    )
    .show(false)

    val companiesJson = List(
      """{"company":"NewCo","employees":[{"firstName":"Sidhartha","lastName":"Ray"},{"firstName":"Pratik","lastName":"Solanki"}]}""",
      """{"company":"FamilyCo","employees":[{"firstName":"Jiten","lastName":"Pupta"},{"firstName":"Pallavi","lastName":"Gupta"}]}""",
      """{"company":"OldCo","employees":[{"firstName":"Vivek","lastName":"Garg"},{"firstName":"Nitin","lastName":"Gupta"}]}""",
      """{"company":"ClosedCo","employees":[]}"""
    )
    val companiesRDD = sparkSession.sparkContext.makeRDD(companiesJson)
    val companiesDF = sparkSession.read.json(companiesRDD)
    companiesDF.createOrReplaceTempView("companies")
    companiesDF.show(false)
    companiesDF.printSchema()

    val employeeDfTemp = sparkSession.sql("select company, explode(employees) as employee from companies")
    employeeDfTemp.show()
    employeeDfTemp.createOrReplaceTempView("employees")
    val employeeDfTemp2 = sparkSession.sql("select company, posexplode(employees) as (employeePosition, employee) from companies")
    employeeDfTemp2.show()
    sparkSession.sql("""
      select
        company,
        employee.firstName as firstName,
        case
          when company = 'FamilyCo' then 'Premium'
          when company = 'OldCo' then 'Legacy'
          else 'Standard'
        end as Tier
      from
        employees
    """)
    .show(false)

    sparkSession.close()
  }
}
