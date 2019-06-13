package org.apache.spark.sql.hive


import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde.serdeConstants

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.orc.OrcConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.util.{SerializableConfiguration, SerializableJobConf}

import scala.collection.mutable.ArrayBuffer

/**
  * @author zyp
  */
//class CacheJson {


class ReadJson(tableName: String,
               jsonKeys: Array[String],
               jsonCols:Array[String],
               sparkSession: SparkSession,
               broadCastedConf: Broadcast[SerializableConfiguration]) {
  var hiveQlTable: Table = null
  var dir: String = null
  val dbName = "default"

  var indexOfJsonPath: String = null
  var jsonPath:String = null

  var jsonColOrders:String = null
  var normalColOrders:String = null

  var originalCacheJsonPathRelationMap:Map[String,String]  =Map.empty

def this(tableName: String,
         jsonKeys: Array[String],
         jsonCols:Array[String],
         allCols:Array[String],
         sparkSession: SparkSession,
         broadCastedConf: Broadcast[SerializableConfiguration]){
  this(tableName, jsonKeys,jsonCols,sparkSession,broadCastedConf)

   val orders = getColOrder(jsonKeys,allCols)
  jsonColOrders = orders._1
  normalColOrders = orders._2
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
  originalCacheJsonPathRelationMap = getOriginalCacheJsonPathRelation()
}

  private def getColOrder(jsonKeys:Array[String],allCols:Array[String]):Tuple2[String,String]={
    var jsonColOrder:ArrayBuffer[Int] = ArrayBuffer.empty
    var normalColOrder:ArrayBuffer[Int] = ArrayBuffer.empty
    for(jsonKey <- jsonKeys){
      jsonColOrder += allCols.indexOf(jsonKey)
    }
    val normalCols = allCols.filter(x => !jsonKeys.contains(x))
    for(normalCol <- normalCols){
      normalColOrder += allCols.indexOf(normalCol)
    }
    (jsonColOrder.mkString(","),normalColOrder.mkString(","))
  }

  private def getOriginalCacheJsonPathRelation():Map[String,String]  ={
    val cacheColOrder = indexOfJsonPath.split(",")
    val oldColOrder = jsonColOrders.split(",")
    (cacheColOrder zip(oldColOrder)).toMap
  }


  def gettableName: String = {
    tableName //原数据库名+_+原表名
  }

 private def composeJsonPath(jsonKeys: Array[String],jsonCols:Array[String]):ArrayBuffer[String]={
   var jsonPaths:ArrayBuffer[String] = ArrayBuffer.empty
   for( i <- 0 until jsonKeys.length ){
     jsonPaths +=  jsonCols(i)+"_"+jsonKeys(i)
   }
   jsonPaths
 }

  lazy val _broadCastedCatchHadoopConf =
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(cacheHadoopConf))

  @transient private lazy val cacheHadoopConf = getCacheJobConf()


  private def getCacheJobConf(): JobConf ={
    val newJobConf = getCloneJobConf()
    newJobConf.set(FileInputFormat.INPUT_DIR,dir)
    newJobConf.set(OrcConf.INCLUDE_COLUMNS.getAttribute,jsonPath)
    newJobConf.set(OrcConf.INCLUDE_COLUMNS.getHiveConfName,indexOfJsonPath)
    newJobConf.set(serdeConstants.LIST_COLUMN_TYPES,"")


//    val deserializer = tableDesc.getDeserializerClass.newInstance
//    deserializer.initialize(newJobConf, tableDesc.getProperties)
//
//    val structOI = ObjectInspectorUtils
//      .getStandardObjectInspector(
//        deserializer.getObjectInspector,
//        ObjectInspectorCopyOption.JAVA)
//      .asInstanceOf[StructObjectInspector]
//
//    val columnTypeNames = structOI
//      .getAllStructFieldRefs.asScala
//      .map(_.getFieldObjectInspector)
//      .map(TypeInfoUtils.getTypeInfoFromObjectInspector(_).getTypeName)
//      .mkString(",")
//
    newJobConf.set(serdeConstants.LIST_COLUMN_TYPES, hiveQlTable.getMetadata.getProperty("columns.types").replace(":",","))
    newJobConf.set(serdeConstants.LIST_COLUMNS, hiveQlTable.getMetadata.getProperty("columns"))


//    newJobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR,"path")
    return newJobConf
  }

  @transient private lazy val tableDesc = new TableDesc(
    hiveQlTable.getInputFormatClass,
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata)

  protected def getCloneJobConf():JobConf = {
    val conf = broadCastedConf.value.value
    val initializeJobConfFunc = HadoopTableReader.initializeLocalJobConfFunc(dir, tableDesc) _
    val initLocalJobConfFuncOpt = Some(initializeJobConfFunc)
    HadoopRDD.CONFIGURATION_INSTANTIATION_LOCK.synchronized {
      val newJobConf = new JobConf(conf)
      if (!conf.isInstanceOf[JobConf]) {
        initLocalJobConfFuncOpt.foreach(f => f(newJobConf))
      }
      newJobConf
    }
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






