package com.itcast.dmp

import com.itcast.dmp.config.AppConfigHelp
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object DmpApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = {
      val conf = new SparkConf()
        .setAppName(AppConfigHelp.Spark_App_Name)
      if (AppConfigHelp.Spark_App_Local_Mode.toBoolean) {
        conf.setMaster(AppConfigHelp.Spark_App_Master)
      } else
        conf
    }
    val spark: SparkSession = {
      //通过隐士转换的方式加载spark application中的配置信息
      import com.itcast.dmp.config.SparkConfigHelper._
      SparkSession.builder()
        .config(sparkConf)
        .loadSparkConfig()
        .getOrCreate()
    }
    Thread.sleep(1000000)
    spark.stop()
  }
}
