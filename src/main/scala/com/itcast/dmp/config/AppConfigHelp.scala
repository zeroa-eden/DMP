package com.itcast.dmp.config

import com.typesafe.config.{Config, ConfigFactory}

object AppConfigHelp {
  lazy val config: Config = ConfigFactory.load()
  lazy val Spark_App_Name: String = config.getString("spark.app.name")
  lazy val Spark_App_Local_Mode: String = config.getString("spark.local.mode")
  lazy val Spark_App_Master: String = config.getString("spark.master")
  lazy val Spark_App_Params: List[(String, String)] = List(
    ("spark.worker.timeout", config.getString("spark.worker.timeout")),
    ("spark.cores.max", config.getString("spark.cores.max")),
    ("spark.rpc.askTimeout", config.getString("spark.rpc.askTimeout")),
    ("spark.network.timeout", config.getString("spark.network.timeout")),
    ("spark.task.maxFailures", config.getString("spark.task.maxFailures")),
    ("spark.speculation", config.getString("spark.speculation")),
    ("spark.driver.allowMultipleContexts", config.getString("spark.driver.allowMultipleContexts")),
    ("spark.serializer", config.getString("spark.serializer")),
    ("spark.buffer.pageSize", config.getString("spark.buffer.pageSize"))
  )
  // kudu parameters
  lazy val KUDU_MASTER: String = config.getString("kudu.master")
}
