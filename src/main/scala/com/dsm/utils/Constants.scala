package com.dsm.utils

import com.typesafe.config.Config

object Constants {
//  val ACCESS_KEY = "<ACCESS_KEY>"
//  val SECRET_ACCESS_KEY = "<SECRET_ACCESS_KEY>"
//  val S3_BUCKET = "<BUCKET_NAME>"
  val ACCESS_KEY = "AKIAVNSAQDMJZHLCSYRK"
  val SECRET_ACCESS_KEY = "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"
  val S3_BUCKET = "scalaexample"
  val ERROR = "ERROR"

  def getRedshiftJdbcUrl(redshiftConfig: Config): String = {
    val host = redshiftConfig.getString("host")
    val port = redshiftConfig.getString("port")
    val database = redshiftConfig.getString("database")
    val username = redshiftConfig.getString("username")
    val password = redshiftConfig.getString("password")
    s"jdbc:redshift://${host}:${port}/${database}?user=${username}&password=${password}"
  }

}
