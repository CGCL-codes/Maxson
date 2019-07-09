package org.apache.spark.examples.orc

import java.util.Date

import org.apache.spark.sql.SparkSession

/**
  * @author zyp
  */
object GetJsonParseTime {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.sql.catalogImplementation","hive")
      //      .config("spark.sql.json.writeCache",true)
      //      .config("spark.sql.codegen.wholeStage", false)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("select * from default_simple").show(1)





  val i = args(0).toDouble

        i match{
          case 0.0 => {
            val tableName = "simple"
            val st = new Date().getTime
            spark.sql(s"select path from $tableName").foreachPartition(iter => println(iter.size))
            val et = new Date().getTime
            println(s"time:${(et-st)/1000.0}s")
          }
          case 0.1 =>{
            val tableName = "simple"
            val st = new Date().getTime
            spark.sql(s"select get_json_object(path,'$$.id') as id from $tableName").foreachPartition(iter => println(iter.size))
            val et = new Date().getTime
            println(s"time:${(et-st)/1000.0}s")
          }
          case 1.0=> {
            val tableName = "medium"
            val st = new Date().getTime
            spark.sql(s"select path from $tableName").foreachPartition(iter => println(iter.size))
            val et = new Date().getTime
            println(s"time:${(et-st)/1000.0}s")
          }
          case 1.1=>{
            val tableName = "medium"
            val st = new Date().getTime
            spark.sql(s"select get_json_object(path,'$$.id') as id, " +
              s"get_json_object(path,'$$.body'),"+
              s"get_json_object(path,'$$.created_at'),"+
              s"get_json_object(path,'$$.updated_at'),"+
              s"get_json_object(path,'$$.html_url')"+
              s"from $tableName").foreachPartition(iter => println(iter.size))
            val et = new Date().getTime
            println(s"time:${(et-st)/1000.0}s")
          }
          case 2.0=> {
            val tableName = "long"
            val st = new Date().getTime
            spark.sql(s"select path from $tableName").foreachPartition(iter => println(iter.size))
            val et = new Date().getTime
            println(s"time:${(et-st)/1000.0}s")
          }
          case 2.1=>{
            val tableName = "long"
            val st = new Date().getTime
            spark.sql(s"select get_json_object(path,'$$.id') as id," +
              s"get_json_object(path,'$$.body'),"+
              s"get_json_object(path,'$$.user'),"+
              s"get_json_object(path,'$$.user.login'),"+
              s"get_json_object(path,'$$.url')"+
              s" from $tableName").foreachPartition(iter => println(iter.size))
            val et = new Date().getTime
            println(s"time:${(et-st)/1000.0}s")
          }
          case 3.0=> {
            val tableName = "huge"
            val st = new Date().getTime
            spark.sql(s"select path from $tableName").foreachPartition(iter => println(iter.size))
            val et = new Date().getTime
            println(s"time:${(et-st)/1000.0}s")
          }
          case 3.1=>{
            val tableName = "huge"
            val st = new Date().getTime
            spark.sql(s"select get_json_object(path,'$$.id') as id," +
              s"get_json_object(path,'$$.user') as path_user,"+
              s"get_json_object(path,'$$.huge.calendar[0].weekday') as path_huge,"+
              s"get_json_object(path,'$$.body'),"+
              s"get_json_object(path,'$$.url')"+

              s" from $tableName").foreachPartition(iter => println(iter.size))
            val et = new Date().getTime
            println(s"time:${(et-st)/1000.0}s")
          }

          case 4.0=>{
            val tableName = "giant"
            val st = new Date().getTime
            spark.sql(s"select path from $tableName").foreachPartition(iter => println(iter.size))
            val et = new Date().getTime
            println(s"time:${(et-st)/1000.0}s")
          }
          case 4.1=>{
            val tableName = "giant"
            val st = new Date().getTime
            spark.sql(s"select time,get_json_object(path,'$$.id') as path_id,get_json_object(path,'$$.url') as path_url," +
              s"get_json_object(path,'$$.user') as path_user,get_json_object(path,'$$.huge.calendar[0].weekday') as path_huge,get_json_object(path,'$$.body') " +
              s"frequency from $tableName").foreachPartition(iter => println(iter.size))
            val et = new Date().getTime
            println(s"time:${(et-st)/1000.0}s")
          }
      }
      }

}
