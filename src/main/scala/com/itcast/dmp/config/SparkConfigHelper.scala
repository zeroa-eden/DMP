package com.itcast.dmp.config

import java.util
import java.util.Map

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.apache.spark.sql.SparkSession


class SparkConfigHelper(builder: SparkSession.Builder) {
  val sparkcConfig: Config = ConfigFactory.load("spark.conf")

  def loadSparkConfig(): SparkSession.Builder = {
    val entrySet: util.Set[Map.Entry[String, ConfigValue]] = sparkcConfig.entrySet()
    import scala.collection.JavaConverters._
    for (entry <- entrySet.asScala) {
      //origin()能获得资源来自哪个文件
      val resource: String = entry.getValue.origin().resource()
      if ("spark.conf".equals(resource)) {
        val key: String = entry.getKey
        //unwrapped解开config的value
        val value: String = entry.getValue.unwrapped().toString
        println(key+""+value)
        builder.config(key, value)
      }
    }
    builder
  }


}


object SparkConfigHelper {
  implicit def bulider2Helper(builder: SparkSession.Builder): SparkConfigHelper = {
    new SparkConfigHelper(builder)
  }
}
