package org.apache.spark.sql.hive

import org.apache.spark.sql.SparkSession

/**
  * @author zyp
  */
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
      val jsonPathInfoArr = line.split(",")
      val dbName = jsonPathInfoArr(0)
      val tableName = jsonPathInfoArr(1)
      val columnName = jsonPathInfoArr(2)
      val jsonPath = jsonPathInfoArr(3)
      (dbName+"."+tableName,columnName+"#"+jsonPath)
    }).reduceByKey((path1,path2) => {
      val path1Arr =  path1.split("#")
      val path2Arr = path2.split("#")
      val wrappedJsonPath1 = s"get_json_object(${path1Arr(0)}, '$$.${path1Arr(1)}') as ${path1Arr(0)}_${path1Arr(1)}"
      val wrappedJsonPath2 = s"get_json_object(${path2Arr(0)}, '$$.${path2Arr(1)}') as ${path2Arr(0)}_${path2Arr(1)}"
      wrappedJsonPath1+","+wrappedJsonPath2
    }).collect().toMap
    for(x <- tableMap){
      val dbAndTableName = x._1.split(".")
      val dbName = dbAndTableName(0)
      val tableName = dbAndTableName(1)
      val selectContentString = x._2
      spark.sql(s"select ${selectContentString} from ${dbName}.${tableName}").write.format("hive").option("fileFormat","orc").saveAsTable(s"${dbName}_${tableName}")
    }
  }
}

//case class JsonPathInfo(dbName:String,tableName:String,columnName:String,jsonPath:String){}
