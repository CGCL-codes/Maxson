package org.apache.spark

/**
  * @author zyp
  */
case class CacheInfo(cachePath:String,//表所在的存储路径
                     jsonColOrders:String,//jsonPath在原来的查询语句中的顺序
                     normalColOrders:String, //普通列在原来的查询语句中的顺序
                     originalCacheJsonPathRelationMap:Map[String,String],//原始查询的jsonPath和缓存的jsonPath的索引的对应关系
                     allCols:Array[String]) {//原表里面所有的列的顺序

}
