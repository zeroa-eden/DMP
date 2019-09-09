package com.itcast.dmp.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class KuduHelper {
  val kuduConfig: Config = ConfigFactory.load("kudu.conf")
  var spark: SparkSession = _
  var kuduContext: KuduContext = _
  var dataframe: DataFrame = _

  def this(sparkSession: SparkSession) {
    this
    this.spark = sparkSession
    this.kuduContext = new KuduContext(kuduConfig.getString("kudu.master"), this.spark.sparkContext)
  }

  def this(df: DataFrame) {
    this(df.sparkSession)
    this.dataframe = df
  }

  //println(kuduConfig)

  def createKuduTable(tableName: String, schema: StructType, keys: Seq[String], isDel: Boolean = true): Unit = {
    println("test............")
    // a. 判断要创建的表是否存在，如果存在的话，判断是否可以删除，如果可以删除，先删除再创建
    if (kuduContext.tableExists(tableName)) {
      if (isDel) {
        kuduContext.deleteTable(tableName)
        //logInfo(s"Kudu中表${tableName}存在，已被删除........")
      } else {
        //logInfo(s"Kudu中表${tableName}存在，不允许删除........")
        return
      }
      println(tableName + "test........")
    } else {
      println(tableName)
      val options: CreateTableOptions = new CreateTableOptions()
      options.setNumReplicas(kuduConfig.getInt("kudu.table.factor"))
      import scala.collection.JavaConverters._
      options.addHashPartitions(keys.asJava, 3)
      kuduContext.createTable(tableName, schema, keys, options)
    }
  }

  def deleteKuduTable(tableName: String) = {
    if (kuduContext.tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
    }
  }

  def readKuduTable(tableName: String): Option[DataFrame] = {
    if (kuduContext.tableExists(tableName)) {
      import org.apache.kudu.spark.kudu._
      val kuduDF = spark.read
        .option("kudu.master", kuduConfig.getString("kudu.master"))
        .option("kudu.table", tableName)
        .kudu
      Some(kuduDF)
    } else {
      None
    }
  }
  def saveAsKuduTable(tableName:String)={
    import org.apache.kudu.spark.kudu._
    dataframe.write
      .mode(SaveMode.Append)
      .option("kudu.master", kuduConfig.getString("kudu.master"))
      .option("kudu.table", tableName)
      .kudu
  }
}

object KuduHelper {
  implicit def sparkSessionToKuduHelper(spark: SparkSession): KuduHelper = {
    new KuduHelper(spark)
  }

  implicit def dataframeToKuduHelper(dataframe: DataFrame): KuduHelper = {
    new KuduHelper(dataframe)
  }
}
