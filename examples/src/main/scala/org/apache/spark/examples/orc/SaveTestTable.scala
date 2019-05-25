package org.apache.spark.examples.orc

import org.apache.spark.sql.SparkSession

/**
  * create with org.apache.spark.examples.orc
  * USER: husterfox
  */
object SaveTestTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .config("spark.sql.catalogImplementation", "hive")
      .config("spark.sql.json.optimize", false)
      .config("spark.network.timeout", 3600)
      .config("spark.sql.codegen.wholeStage", false)
      .config("spark.sql.json.writeCache",true)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val tableName = "giantPath"
    val df = spark.sparkContext.textFile("/Users/husterfox/workspace/SparkDemo/train.txt").map(x => {
      val info = x.split("\\*")
      Log1(info(0), info(1).toInt, info(2))
    }).toDF()
    spark.sql(s"drop table if exists $tableName")
    df.write.format("hive").option("fileFormat", "orc").saveAsTable(s"$tableName")

    val log_path = spark.sql(s"select get_json_object(path,'$$.id') as path_id,get_json_object(path,'$$.html_url')as path_body from $tableName")
    spark.sql(s"drop table if exists default_$tableName")
    log_path.write.format("hive").option("fileFormat", "orc").saveAsTable(s"default_$tableName")
  }

  case class Log1(path: String, frequency: Int, time:String)
}
