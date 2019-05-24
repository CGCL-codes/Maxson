package org.apache.spark.examples.orc

import java.util.Date

import org.apache.spark.sql.SparkSession

/**
  * @author zyp
  */
object TestUtil {

   def createTable(spark: SparkSession,path:String,tableName:String):Unit={
     import spark.implicits._
     val  df = spark.sparkContext.textFile(path).map(x => {
       val info = x.split("\\*")
       Log(info(0),info(2),info(1).toInt)
     }).toDF()
     spark.sql(s"DROP TABLE IF EXISTS $tableName")
     df.write.format("hive").option("fileFormat","orc").saveAsTable(tableName)
   }

  def cacheJson(spark: SparkSession,cacheTableName:String,tableName:String):Unit={
    val log_path = spark.sql(s"select get_json_object(path,'$$.id') as path_id,get_json_object(path,'$$.url') as path_url from $tableName")
    spark.sql(s"DROP TABLE IF EXISTS $cacheTableName")
    log_path.write.format("hive").option("fileFormat","orc").saveAsTable(cacheTableName)
  }


  /****************************************************关闭优化：读两列json的所耗时间*************************************************************/

  def readJson(sparkSession: SparkSession,tableName:String,times:Int):Double={
    val startTime = new Date().getTime
    for(i <-0 until times){
      val log = sparkSession.sql(s"select get_json_object(path,'$$.id') as path_id,get_json_object(path,'$$.url') as path_url from $tableName")
      log.foreach(x =>{
        x.get(0)
        x.get(1)
      })
    }
    val endTime = new Date().getTime
    (endTime-startTime)/(times.toDouble*1000)
  }

  /*****************************************************打开优化：读两列Json缓存的所耗时间*********************************************************/

  def readCacheJson(sparkSession: SparkSession,cacheTableName:String,times:Int):Double={
    val startTime = new Date().getTime
    for(i <-0 until times){
      val log = sparkSession.sql(s"select path_id,path_url from $cacheTableName")
      log.foreach(x =>{
        x.get(0)
        x.get(1)
      })
    }
    val endTime = new Date().getTime
    (endTime-startTime)/(times.toDouble*1000)
  }

  /***************************************************读普通列和json列的所耗时间*************************************************/
  def readCol(sparkSession: SparkSession,tableName:String,times:Int):Double={
    val startTime = new Date().getTime
    for(i <-0 until times){
      val log = sparkSession.sql(s"select time,get_json_object(path,'$$.id') as path_id,get_json_object(path,'$$.url') as path_url,frequency from $tableName")
//      log.foreachPartition(iter => println(iter.size))
      log.show(10)
    }
    val endTime = new Date().getTime
    (endTime-startTime)/(times.toDouble*1000)
  }

}
case class Log(path:String,time:String,frequency: Int)


