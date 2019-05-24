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
      .config("spark.sql.json.optimize",true)
      .enableHiveSupport()
      .getOrCreate()

    val startTime = new Date().getTime
//    val log = spark.sql("select path from newLog ")
//    val log = spark.sql("select get_json_object(path,'$.name')as path_name from newLog ")
//    val log = spark.sql("select frequency,get_json_object(path,'$.age') as path_age from newLog ")
//  val log = spark.sql("select path_name,path_age from default_newLog")
    val log = spark.sql("select frequency,time,get_json_object(path,'$.age') as path_age,get_json_object(path,'$.name')as path_name from newLog")
    log.foreach(x=>{
      x.get(0)
      x.get(1)
      x.get(2)
      x.get(3)
    })
    val endTime = new Date().getTime
    println(endTime-startTime)
  }
}
