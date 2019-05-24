package org.apache.spark.examples.orc

import org.apache.spark.sql.SparkSession

/**
  * @author zyp
  */
object Huge {
  val path = "C:\\Users\\zyp\\ali\\spark\\examples\\src\\main\\resources\\huge.txt"
  val tableName = "huge2"
  val cacheTableName = "default_huge2"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[10]")
      .config("spark.sql.catalogImplementation","hive")
      .config("spark.sql.json.optimize",true)
      .config("spark.sql.json.writeCache",true)
//      .config("spark.sql.codegen.wholeStage", false)
      .enableHiveSupport()
      .getOrCreate()

//    TestUtil.createTable(spark,path,tableName)
//    TestUtil.cacheJson(spark,cacheTableName,tableName)
//    spark.sql(s"DROP TABLE IF EXISTS $cacheTableName")


//    val readCacheJsonTime  = TestUtil.readCacheJson(spark,cacheTableName,5)
//    println(s"readCacheJsonTime:$readCacheJsonTime")


//    val readJsonTime = TestUtil.readJson(spark,tableName,1)
//    println(s"readJsonTime:$readJsonTime")

////
    val readColTime = TestUtil.readCol(spark,tableName,1)
    println(s"readColTime:$readColTime")

  }




}

