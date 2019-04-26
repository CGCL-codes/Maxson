package org.apache.spark.examples.orc


import org.apache.spark.sql.{SaveMode, SparkSession}

object SaveORC {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.sql.catalogImplementation","hive")
      .enableHiveSupport()
      .getOrCreate()
//    import spark.implicits._
//    val  df = spark.sparkContext.textFile("examples/src/main/resources/verify2.txt").map(x => {
//      val info = x.split(",")
//      Log(info(0),info(1).toInt,info(2))
//    }).toDF()
//    df.createOrReplaceTempView("logView")
//    val df2 = spark.sql("select * from logView order by frequency").rdd
//   df2.coalesce(1).saveAsTextFile("examples/src/main/resources/verify2.txt")

 //   spark.sql("drop table log")
//    df.write.format("hive").option("fileFormat","orc").saveAsTable("log")

//    spark.sql("insert overwrite table log select * from log order by time")
    /**********************搞清楚RowGroup是怎么跳的***************************/
//   spark.sql("select count(*) from log").show()
//   val rdd =  spark.sql("select path,frequency from log where frequency >= 3").rdd
//   println(rdd.toDebugString)
//    rdd.collect()

    /*****************模拟从原表读数据并缓存**************************/
//    val log = spark.sql("select frequency,time from log")
//    log.collect()
//    log.write.format("hive").option("fileFormat","orc").saveAsTable("log_path")
//    log.write.format("orc").save("data/log_path")
    /*********************模拟读缓存*************************/
//    val log = spark.read.orc("data/log_path")
    val log  = spark.sql("select path from log_path")
    log.collect()
    log.show(10)
  }
}

case class People(name:String,age:Long)
case class Log(path:String,frequency: Int,time:String)
