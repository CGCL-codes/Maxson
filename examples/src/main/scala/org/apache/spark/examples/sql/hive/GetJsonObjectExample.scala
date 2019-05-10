package org.apache.spark.examples.sql.hive

import org.apache.spark.sql.execution.debug._
import java.io.File

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * create with org.apache.spark.examples.sql.hive
  * USER: husterfox
  */
object GetJsonObjectExample {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession.builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.optimize.json.enable", true)
      .config("spark.network.timeout", 3600)
      .config("spark.sql.codegen.wholeStage",false)
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .config("spark.sql.json.optimize",true)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val value = Seq((
      """{"name":"fox",
        |"age":12,
        |"pro":"InMemoryComputing"}""".stripMargin,1,"something","20190101"),(
      """{"name":"fox1",
        |"age":13,
        |"pro":"InMemoryComputing"}""".stripMargin,1,"something","20190102"))
    spark.sql("""drop table if exists json_db""")
    spark.sql(
      """create table if not exists json_db(
        |info STRING,
        |id   INT,
        |extra STRING)
        |partitioned by(ds STRING)
        |STORED AS ORC
      """.stripMargin)
    val df = spark.sparkContext.parallelize(value).toDF()
    df.write.mode(SaveMode.Overwrite).insertInto("json_db")
    spark.sql(
      """
        |insert into table json_db partition(ds) values ('{"name":"yipeng","age":13, "pro":"InMemoryComputing"}', 1, 'something', '20190101')
      """.stripMargin)

    val df0 = spark.sql("select  get_json_object(info,'$.age')  as col1, extra, id from json_db where ds >= '20190101'")
    df0.show()
    df0.explain(true)
  }

}
