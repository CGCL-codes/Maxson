package org.apache.spark.examples.orc

import org.apache.spark.sql.{SaveMode, SparkSession}

object SaveOrcHu {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "hive")
      .config("spark.sql.json.optimize",false)
      .config("spark.network.timeout", 3600)
      .config("spark.sql.codegen.wholeStage", false)
      .enableHiveSupport()
      .getOrCreate()
    //    import spark.implicits._
    //    val df = spark.sparkContext.textFile("/Users/husterfox/workspace/SparkDemo/train.txt").map(x => {
    //      val info = x.split("\\*")
    //      Log(info(0), info(1).toInt, info(2))
    //    }).toDF()
    //
    //
    //    spark.sql("drop table if exists hugePath")
    //    df.write.format("hive").option("fileFormat", "orc").saveAsTable("hugePath")

    //    spark.sql("insert overwrite table log select * from log order by time")
    /** ********************搞清楚RowGroup是怎么跳的 ***************************/
    //   spark.sql("select count(*) from log").show()
    //   val rdd =  spark.sql("select path,frequency from log where frequency >= 3").rdd
    //   println(rdd.toDebugString)
    //    rdd.collect()

    /** ***************模拟从原表读数据并缓存 **************************/
    //    val log_path = spark.sql("select get_json_object(path,'$.id') as path_id,get_json_object(path,'$.body')as path_body from hugePath")
    //    log_path.write.format("hive").option("fileFormat", "orc").saveAsTable("default_hugePath")
    /** *******************模拟读缓存(测试单独读path的reader可不可以用) *************************/
    //    val log = spark.read.orc("data/log_path")
    //    val log  = spark.sql("select path from newLog_path")
    ////    log.collect()
    ////    log.show(10)
    /** ********************模拟读缓存，当语句中有path的时候，开启两个reader ************************/

    val tableName = "largePath"
    val num = 1
    val optimize = spark.sparkContext.getConf.getBoolean("spark.sql.json.optimize",false)
    val time: Array[Long] = new Array[Long](num)
    //    spark.sql(s"select * from default_$tableName").show()
    //    spark.sql(s"select * from $tableName").show()
//
    spark.sql(s"select count(distinct(path)) from $tableName group by frequency").explain(true)
//    for (i <- 0 until num) {
//      val start = System.currentTimeMillis()
//      spark.sql(
//        s"""select frequency, get_json_object(path,'$$.id')as path_id,
//           |get_json_object(path,'$$.html_url') as path_body,
//           |time
//           | from $tableName""".stripMargin)
//        //        .show(10)
//        .foreachPartition(iter => count.add(iter.size))
//      val end = System.currentTimeMillis()
//      time(i) = (end - start) / 1000
//    }
//    val optimizeStr = if (optimize) {
//      "optimize"
//    } else {
//      "unoptimize"
//    }
//    println(s"""$optimizeStr $num times: ${time.mkString(",")}  avg: ${time.sum / num} count: ${count.value}""")
  }
}

//case class People(name:String,age:String)

