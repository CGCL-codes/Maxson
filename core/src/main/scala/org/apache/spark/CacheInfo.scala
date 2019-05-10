package org.apache.spark

/**
  * @author zyp
  */
case class CacheInfo(cachePath:String,//表所在的存储路径
                     tableName:String,//表名
                     jsonPath:String,//jsonPath
                     columns:String,//缓存表中所有的列
                     indexOfJsonPath:String,//需要查询的jsonPath在schema里面的索引（第几列）
                     allCols:String) {//原表里面所有的列的顺序

  def getCachePath:String={
    cachePath
  }
  def getTableName:String = {
    tableName
  }
  def getJsonPath:String = {
    jsonPath
  }
  def getColumns:String = {
    columns
  }
  def getIndexOfJsonPath:String = {
    indexOfJsonPath
  }

}
