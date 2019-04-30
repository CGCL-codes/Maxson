package org.apache.spark.sql.hive

import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.hive.client.HiveClientImpl

/**
  * @author zyp
  */
class CacheJson {

  class WriteJson{
    /**
      * 对每张表的path提交一个查询任务并缓存
      */
    def CacheJsonPath(path:String) :Unit = {
      val spark = SparkSession
        .builder()
        .master("local")
        .config("spark.sql.catalogImplementation","hive")
        .enableHiveSupport()
        .getOrCreate()
      val tableMap = spark.sparkContext.textFile(path).map(line => {
        val jsonPathInfo = line.split(",")
        val dbName = jsonPathInfo(0)
        val tableName = jsonPathInfo(1)
        val jsonPath = jsonPathInfo(2)
        (dbName+"."+tableName,jsonPath)
      }).reduceByKey((path1,path2) => {
        path1+","+path2
      }).collect().toMap
      for(x <- tableMap){
        val dbandTableName = x._1
        val jsonPath = x._2
        spark.sql(s"select ${jsonPath} from ${dbandTableName}").write.format("hive").option("fileFormat","orc").saveAsTable(s"${dbandTableName}")
      }
    }
  }

  class ReadJson(dbName:String,tableName:String,jsonPath:String){
    var hiveQlTable:Table= null
    var dir:String = null
    var indexOfJsonPath:Int = 0

    /**
      * @param sparkSession
      * @return
      */
    def jsonPathExists(sparkSession: SparkSession): Boolean={
      var flag:Boolean = false
      try {
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
  }
}





