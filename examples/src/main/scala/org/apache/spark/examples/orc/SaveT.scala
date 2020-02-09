package org.apache.spark.examples.orc

import org.apache.spark.sql.SparkSession

/**
  * create with org.apache.spark.examples.orc
  * USER: husterfox
  */
object SaveT {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.sql.json.writeCache",value = true)
      .config("spark.sql.catalogImplementation", "hive")
      .master("local").getOrCreate()
    import  spark.implicits._
    spark.sql("drop table if exists T")
    val df = spark.sparkContext.parallelize(Seq(PaperSchema("""non_json_column0""", """non_json_column1""", """{"id":10000, "url":"https"}""")))
      .toDF("non_json_column0","non_json_column1","json_column0")

    df.write.format("hive").option("fileFormat","orc").saveAsTable("T")


    spark.sql("drop table if exists default_T")

    val cache = spark.sql(
      """
        |select get_json_object(json_column0,'$.id') as json_column0_id,
        |get_json_object(json_column0,'$.url') as json_column0_url
        |from T
      """.stripMargin)
    cache.write.format("hive").option("fileFormat","orc").saveAsTable("default_T")
  }
}
