package org.apache.spark.sql.hive

import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.hive.client.HiveClientImpl

/**
  * @author zyp
  */
//class CacheJson {



  class ReadJson(tableName:String,jsonPath:String){
    var hiveQlTable:Table= null
    var dir:String = null
    var indexOfJsonPath:Int = 0
    val dbName =  "default"

  def getJsonPath:String ={
    jsonPath          //原表中的列名+_+原表中的jsonPath
  }

  def gettableName:String={
    tableName        //原数据库名+_+原表名
  }


    /**
      * @param sparkSession
      * @return
      */
    def jsonPathExists(sparkSession: SparkSession): Boolean={
      var flag:Boolean = false
      try {
        //TODO dbname 是我们自己确定的
        val catalogTable = sparkSession.sessionState.catalog.externalCatalog.getTable(dbName, tableName)
        hiveQlTable = HiveClientImpl.toHiveTable(catalogTable)
        val columns = hiveQlTable.getMetadata.getProperty("columns").split(",").toList
        if(columns.contains(jsonPath)){
          flag = true
          indexOfJsonPath = columns.indexOf(jsonPath)
          dir = hiveQlTable.getPath.toString
        }
      }catch{
        case e:Exception => println(s"Table or view '$tableName' not found in database '$dbName'")
      }
      flag
    }

  def jsonPathExists(sparkSession: SparkSession,tableName:String,jsonPath:String): Boolean={
    var flag:Boolean = false
    try {
      //TODO dbname 是我们自己确定的
      val catalogTable = sparkSession.sessionState.catalog.externalCatalog.getTable(dbName, tableName)
      hiveQlTable = HiveClientImpl.toHiveTable(catalogTable)
      val columns = hiveQlTable.getMetadata.getProperty("columns").split(",").toList
      if(columns.contains(jsonPath)) flag = true
    }catch{
      case e:Exception => println(s"Table or view '$tableName' not found in database '$dbName'")
    }
    flag
  }
  }
//}





