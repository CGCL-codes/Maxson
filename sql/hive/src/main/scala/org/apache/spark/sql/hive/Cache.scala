package org.apache.spark.sql.hive

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{InputFormat, JobConf}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.util.{ReflectionUtils, StringUtils}
import org.apache.spark.Partition
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config.HADOOP_RDD_IGNORE_EMPTY_SPLITS
import org.apache.spark.rdd.HadoopPartition
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.hive.client.HiveClientImpl

class Cache(sparkSession:SparkSession) {

  private def sessionCatalog: SessionCatalog = sparkSession.sessionState.catalog
  var path:String = null
  var dir:String = null
  var catalogTable: CatalogTable =null
  val hiveQlTable = HiveClientImpl.toHiveTable(catalogTable)
  val ifc = hiveQlTable.getInputFormatClass
    .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
  private val ignoreEmptySplits = sparkSession.sparkContext.conf.get(HADOOP_RDD_IGNORE_EMPTY_SPLITS)
  var id = 0


  /**
    * @param tableName
    * @return
    */
  def tableExists(tableName:String,dbName:String): Boolean={
    var flag:Boolean = false
//    val tableIdent = TableIdentifier(tableName, Option(dbName))
//    sessionCatalog.isTemporaryTable(tableIdent) || sessionCatalog.tableExists(tableIdent)
    try {
     catalogTable = sessionCatalog.externalCatalog.getTable(dbName, tableName)
    }catch{
      case e:Exception => println(s"Table or view '$tableName' not found in database '$dbName'")
    }finally {
      if(catalogTable != null){
        flag = true
        dir = catalogTable.storage.locationUri.get.getPath
      }
    }
   flag
  }

  def JudgeCache(tableName:String,dbName:String): Unit ={
    if(!tableExists(tableName,dbName)){
       //TODO 如果表不存在
    }else{

    }
  }
  def getPartitions(dirs:String): Array[Partition] ={
    val conf = new Configuration(false)
    conf.set(FileInputFormat.INPUT_DIR,dirs)
    val jobConf = new JobConf(conf)
    SparkHadoopUtil.get.addCredentials(jobConf)

    val allInputSplits = getInputFormat(jobConf).getSplits(jobConf, 0)

    val inputSplits = if (ignoreEmptySplits) {
      allInputSplits.filter(_.getLength > 0)
    } else {
      allInputSplits
    }
    val array = new Array[Partition](inputSplits.size)
    for (i <- 0 until inputSplits.size) {
      array(i) = new HadoopPartition(id , i, inputSplits(i))
      id +=1
    }
    array
  }
  def getInputFormat[K,V](conf: JobConf): InputFormat[K, V] = {
    val newInputFormat = ReflectionUtils.newInstance(ifc.asInstanceOf[Class[_]], conf)
      .asInstanceOf[InputFormat[K, V]]
    newInputFormat match {
      case c: Configurable => c.setConf(conf)
      case _ =>
    }
    newInputFormat
  }
}
