package org.apache.spark.sql.hive

import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.hive.client.HiveClientImpl

import scala.collection.mutable.ArrayBuffer

/**
  * @author zyp
  */
//class CacheJson {


class ReadJson(tableName: String, jsonKeys: Array[String],jsonCols:Array[String]) {
  var hiveQlTable: Table = null
  var dir: String = null
  var indexOfJsonPath: String = null
  val dbName = "default"
  var jsonPath:String = null
  var jsonColOrders:String = null

def this(tableName: String, jsonKeys: Array[String],jsonCols:Array[String],allCols:Array[String],sparkSession: SparkSession){
  this(tableName, jsonKeys,jsonCols)

  jsonColOrders = getJsonColOrder(jsonCols,allCols)
  val jsonPaths:ArrayBuffer[String] = composeJsonPath(jsonKeys,jsonCols)
  var jsonPathsOrder:ArrayBuffer[Int] = ArrayBuffer.empty

  val catalogTable = sparkSession.sessionState.catalog.externalCatalog.getTable(dbName, tableName)
  hiveQlTable = HiveClientImpl.toHiveTable(catalogTable)
  dir = hiveQlTable.getPath.toString
  val columns = hiveQlTable.getMetadata.getProperty("columns").split(",").toList
  for(jsonPath <- jsonPaths){
    jsonPathsOrder += columns.indexOf(jsonPath)
  }
  val sortedCols = (jsonPathsOrder zip(jsonPaths)).sortBy(_._1).unzip
  jsonPath = sortedCols._2.mkString(",")
  indexOfJsonPath = sortedCols._1.mkString(",")
}

  def getJsonColOrder(jsonCols:Array[String],allCols:Array[String]):String={
    var jsonColOrder:ArrayBuffer[Int] = ArrayBuffer.empty
    for(jsonCol <- jsonCols){
      jsonColOrder += allCols.indexOf(jsonCol)
    }
    jsonColOrder.mkString(",")
  }
  def gettableName: String = {
    tableName //原数据库名+_+原表名
  }

 def composeJsonPath(jsonKeys: Array[String],jsonCols:Array[String]):ArrayBuffer[String]={
   var jsonPaths:ArrayBuffer[String] = ArrayBuffer.empty
   for( i <- 0 until jsonKeys.length ){
     jsonPaths +=  jsonCols(i)+"_"+jsonKeys(i)
   }
   jsonPaths
 }
}

object ReadJson{
  def jsonPathExists(sparkSession: SparkSession, tableName: String, jsonPath: String): Boolean = {
    var flag: Boolean = false
    val dbName = "default"
    try {
      //TODO dbname 是我们自己确定的
      val catalogTable = sparkSession.sessionState.catalog.externalCatalog.getTable(dbName, tableName)
      val hiveQlTable = HiveClientImpl.toHiveTable(catalogTable)
      val columns = hiveQlTable.getMetadata.getProperty("columns").split(",").toList
      if (columns.contains(jsonPath)) flag = true
    } catch {
      case e: Exception => println(s"Table or view '$tableName' not found in database '$dbName'")
    }
    flag
  }
}






