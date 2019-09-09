package com.itcast.dmp.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object SparkKuduTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()
    import spark.implicits._
    val tableName = "kudu_student"
    val schema = StructType(
      Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = false)
      )
    )
    val keys: Seq[String] = Seq("id")
    import com.itcast.dmp.utils.KuduHelper._
    //    spark.createKuduTable(tableName, schema, keys, isDel = false)
    val studentsDF: DataFrame = Seq(
      (10001, "zhangsan", 23), (10002, "lisi", 22), (10003, "wagnwu", 23),
      (10004, "xiaohong", 21), (10005, "tainqi", 235), (10006, "zhaoliu", 24)
    ).toDF("id", "name", "age")
    //    studentsDF.saveAsKuduTable(tableName)
    spark.readKuduTable(tableName) match {
      case Some(df) => df.show(10, truncate = false)
      case None => println("没有数据。。。。。。。。")
    }
    spark.stop()
  }
}
