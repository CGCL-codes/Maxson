package org.apache.spark.examples.orc

import java.util.Date

import org.apache.spark.sql.SparkSession

/**
  * @author zyp
  */
object PerformanceTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.sql.catalogImplementation","hive")
      .config("spark.sql.json.optimize",false)
      .enableHiveSupport()
      .getOrCreate()

    val startTime = new Date().getTime
    val log = spark.sql("select frequency,time,get_json_object(path,'$.name')as path_name,get_json_object(path,'$.age') as path_age from newLog " +
      "where get_json_object(path,'$.age') is null or get_json_object(path,'$.name') is null")

    log.show(10)
    val endTime = new Date().getTime
    println(endTime-startTime)
  }
}
